import argparse
import hashlib
import json
from urllib.parse import urlencode, urlsplit, urlunsplit

from .stream import StreamError, StreamReceiver
from .store import Store
from .transport import BudgetExceeded, WireStreamer
from .decode import decode
from .transport import fetch


def collect_one(url, database, *, method='GET', headers=None, body=None, max_bytes=1048576):
    response=fetch(url,method=method,headers=headers,body=body,max_bytes=max_bytes)
    if not 200 <= response.status < 300:
        raise ValueError(f'source status {response.status}')
    rows=decode(response.body,dict(response.headers))
    receipt=hashlib.sha256(url.encode()+b'\0'+response.body).hexdigest()
    store=Store(database)
    try:
        store.save_response(receipt,response,rows)
        return dict(receipt=receipt,count=len(rows),documents=store.documents())
    finally:
        store.close()


class AcquisitionCancelled(Exception):
    """Raised by a ``should_stop`` callback to end reception early."""


def _stream_url(url, from_segment):
    """Replace/insert ``from`` on the stream URL, keeping the rest intact."""
    parts=urlsplit(url)
    query=[(k,v) for k,v in
           (item.split('=',1) if '=' in item else (item,'')
            for item in parts.query.split('&') if item)]
    query=[(k,v) for k,v in query if k!='from']
    query.append(('from',from_segment))
    return urlunsplit((parts.scheme,parts.netloc,parts.path,
                       urlencode(query),parts.fragment))


def stream_collect(url, database, *, source=None, from_segment=None,
                   method='GET', headers=None, body=None, timeout=5,
                   max_wire_bytes=1048576, max_decoded_bytes=4194304,
                   max_open_record=1048576, should_stop=None, on_segment=None):
    """Receive an RFC 7464 stream, saving each verified segment as it completes.

    ``from_segment`` defaults to the persisted cursor: a segment that never
    verified stays the cursor target, so a resuming caller cannot skip it.
    ``should_stop`` is polled before every network read and returns true to
    abort; :class:`AcquisitionCancelled` is raised at that point.

    ``on_segment`` receives the summary of each segment the instant it
    verifies, before the connection closes.

    The raw entity bytes read are always kept as evidence on the attempt row,
    including partial reads stopped by a budget, cancellation or socket error.
    The return value describes the attempt and everything persisted so far;
    exceptions carry the attempt id in ``exc.attempt``.
    """
    source=source or url
    store=Store(database)
    try:
        if from_segment is None:
            from_segment=store.stream_cursor(source)
        if from_segment is None:
            # A pristine cursor: derive the start from the URL's own ``from``
            # so the first attempt is recorded against the right boundary.
            for item in urlsplit(url).query.split('&'):
                key,_,value=item.partition('=')
                if key=='from' and value:
                    from_segment=value
                    break
        request_url=_stream_url(url,from_segment) if from_segment else url
        attempt_id=None
        receiver=None
        wire=None
        try:
            wire=WireStreamer(
                request_url, method=method, headers=headers, body=body,
                max_wire_bytes=max_wire_bytes,
                max_decoded_bytes=max_decoded_bytes, timeout=timeout)
            with wire:
                attempt_id=store.start_stream_attempt(
                    source, request_url, from_segment,
                    content_encoding=wire.content_encoding)
                if not 200 <= wire.response.status < 300:
                    store.finish_stream_attempt(
                        attempt_id, status=wire.response.status,
                        outcome='http-error',
                        reason='http-status',
                        detail=f'HTTP {wire.response.status}',
                        wire_bytes=0, decoded_bytes=0, raw=b'')
                    raise ValueError(f'source status {wire.response.status}')
                receiver=StreamReceiver(
                    source, request_url, store=store, attempt_id=attempt_id,
                    max_open_record=max_open_record, on_commit=on_segment)
                while True:
                    if should_stop is not None and should_stop():
                        raise AcquisitionCancelled('cancelled by caller')
                    event=wire.read()
                    if event.eof:
                        break
                    receiver.feed(event.chunk)
                receiver.finish()
            # Verified segments were committed the moment their end record
            # was validated; a stream that ends without one is an attempt-level
            # problem, and the receiver has already saved the open segment.
            segments=store.stream_segments(source)
            cursor=store.stream_cursor(source)
            open_segment=receiver.final_open
            if open_segment is not None:
                # Bytes from this attempt form a segment whose end record
                # never arrived; the receiver saved it as partial/corrupt.
                outcome='incomplete'
                reason=('open-record-at-eof' if open_segment.get('open_record')
                        else 'end-record-missing')
                detail=(f"segment {open_segment['segment']!r} did not verify "
                        f"at end of stream")
            else:
                outcome='complete'; reason=''; detail=''
            store.finish_stream_attempt(
                attempt_id, status=wire.response.status, outcome=outcome,
                reason=reason or '', detail=detail,
                wire_bytes=wire.wire_bytes, decoded_bytes=wire.decoded_bytes,
                raw=bytes(wire.raw), raw_truncated=wire.raw_truncated)
            result=dict(attempt_id=attempt_id, source=source,
                        request_url=request_url, from_segment=from_segment,
                        status=wire.response.status, outcome=outcome,
                        reason=reason, detail=detail,
                        wire_bytes=wire.wire_bytes,
                        decoded_bytes=wire.decoded_bytes,
                        raw_truncated=wire.raw_truncated,
                        cursor=cursor, segments=segments)
            return result
        except BudgetExceeded as exc:
            _finish_failure(store, attempt_id, receiver, wire, exc.budget, str(exc))
            raise
        except AcquisitionCancelled as exc:
            _finish_failure(store, attempt_id, receiver, wire, 'cancelled', str(exc))
            raise
        except ConnectionError as exc:
            _finish_failure(store, attempt_id, receiver, wire, 'connection-error', str(exc))
            raise
        except StreamError as exc:
            _finish_failure(store, attempt_id, receiver, wire, exc.reason, exc.detail)
            raise
        except Exception as exc:
            _finish_failure(store, attempt_id, receiver, wire, 'error', repr(exc))
            raise
    finally:
        store.close()


def _finish_failure(store, attempt_id, receiver, wire, reason, detail):
    """Close framing state and record why reception stopped.

    The receiver saves its open segment as partial/corrupt before the attempt
    row is updated, so already-verified segments and the damaged range both
    survive an abort.
    """
    if attempt_id is None:
        return
    if receiver is not None and not receiver.finished:
        try:
            receiver.finish()
        except Exception:
            pass
    evidence=store.stream_evidence(attempt_id, include_raw=False) or {}
    raw=bytes(wire.raw) if wire is not None else b''
    store.finish_stream_attempt(
        attempt_id, status=evidence.get('status'), outcome='aborted',
        reason=reason, detail=detail,
        wire_bytes=evidence.get('wire_bytes', 0),
        decoded_bytes=evidence.get('decoded_bytes', 0), raw=raw,
        raw_truncated=True)


def stream_show(database, *, source=None, segment=None, attempt=None):
    """Inspect persisted stream state: segments, ranges, damage, evidence."""
    store=Store(database)
    try:
        if segment is not None:
            found=store.stream_segment(segment)
            if found is None:
                raise ValueError(f'unknown segment {segment!r}')
            return found
        if attempt is not None:
            found=store.stream_evidence(attempt)
            if found is None:
                raise ValueError(f'unknown attempt {attempt!r}')
            return found
        return dict(source=source, cursor=store.stream_cursor(source) if source else None,
                    segments=store.stream_segments(source),
                    attempts=store.stream_attempts(source))
    finally:
        store.close()


def main():
    parser=argparse.ArgumentParser(
        description='Receive an RFC 7464 arrival-event stream incrementally.')
    parser.add_argument('url')
    parser.add_argument('database')
    parser.add_argument('--method',default='GET')
    parser.add_argument('--source',default=None,
                        help='logical source name (defaults to the URL)')
    parser.add_argument('--from',dest='from_segment',default=None,
                        help='segment boundary to replay from (default: stored cursor)')
    parser.add_argument('--max-wire-bytes',type=int,default=1048576)
    parser.add_argument('--max-decoded-bytes',type=int,default=4194304)
    parser.add_argument('--max-open-record',type=int,default=1048576)
    parser.add_argument('--timeout',type=float,default=5)
    parser.add_argument('--show',action='store_true',
                        help='print persisted segments/attempts instead of receiving')
    parser.add_argument('--segment',default=None,
                        help='with --show: details and raw records for one segment')
    parser.add_argument('--attempt',type=int,default=None,
                        help='with --show: stored raw evidence for one attempt')
    args=parser.parse_args()
    if args.show:
        overview=stream_show(args.database,source=args.source or args.url,
                             segment=args.segment,attempt=args.attempt)
        print(json.dumps(overview,ensure_ascii=False,default=lambda b: b.hex()))
        return
    try:
        result=stream_collect(
            args.url,args.database,source=args.source,
            from_segment=args.from_segment,method=args.method,
            timeout=args.timeout,max_wire_bytes=args.max_wire_bytes,
            max_decoded_bytes=args.max_decoded_bytes,
            max_open_record=args.max_open_record)
    except (ValueError,ConnectionError) as exc:
        print(json.dumps(dict(error=str(exc)),ensure_ascii=False))
        raise SystemExit(1)
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    main()
