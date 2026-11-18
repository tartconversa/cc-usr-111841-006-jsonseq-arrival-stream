from dataclasses import dataclass
import hashlib
import urllib.request
import urllib.error
import zlib

@dataclass(frozen=True)
class Response:
    url: str
    status: int
    headers: tuple
    body: bytes

    def header(self, name, default=''):
        return ', '.join(v for k,v in self.headers if k.lower()==name.lower()) or default

    @property
    def sha256(self):
        return hashlib.sha256(self.body).hexdigest()

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

def fetch(url, *, method='GET', headers=None, body=None, max_bytes=1048576, timeout=5):
    if max_bytes < 0:
        raise ValueError('max_bytes must be nonnegative')
    request=urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    opener=urllib.request.build_opener(NoRedirect)
    try:
        stream=opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        stream=exc
    with stream:
        data=stream.read(max_bytes+1)
        if len(data)>max_bytes:
            raise ValueError('response exceeds max_bytes')
        return Response(url, stream.status, tuple(stream.headers.items()), data)


class DecodeError(ValueError):
    """The entity cannot be decompressed as announced."""

    def __init__(self, reason, detail=''):
        super().__init__(detail or reason)
        self.reason=reason
        self.detail=detail


class StreamDecoder:
    """Decompress ``Content-Encoding: identity`` or ``gzip`` incrementally.

    ``feed`` may split a gzip member, header or trailer at any byte; ``finish``
    rejects a member whose trailer never arrived.  Anything after the end of
    the gzip member is treated as a protocol error, not silently dropped.
    """

    def __init__(self, content_encoding=''):
        encodings=[e.strip().lower() for e in (content_encoding or '').split(',') if e.strip()]
        if not encodings or encodings==['identity']:
            self.kind='identity'
            self._zlib=None
        elif encodings in (['gzip'], ['x-gzip']):
            self.kind='gzip'
            self._zlib=zlib.decompressobj(16+zlib.MAX_WBITS)
        else:
            raise DecodeError('unsupported-encoding', content_encoding or '(empty)')
        self.finished=False

    def feed(self, chunk):
        if not chunk:
            return b''
        if self._zlib is None:
            return bytes(chunk)
        try:
            data=self._zlib.decompress(chunk)
        except zlib.error as exc:
            raise DecodeError('gzip-invalid', str(exc)) from exc
        if self._zlib.unused_data:
            raise DecodeError('gzip-trailing-data',
                              f'{len(self._zlib.unused_data)} byte(s) after gzip member')
        return data

    def finish(self):
        if self._zlib is None or self.finished:
            return b''
        self.finished=True
        try:
            data=self._zlib.flush()
        except zlib.error as exc:
            raise DecodeError('gzip-invalid', str(exc)) from exc
        if not self._zlib.eof:
            raise DecodeError('gzip-truncated', 'gzip member ended before its trailer')
        return data


@dataclass(frozen=True)
class WireRead:
    """One (possibly empty) result from :class:`WireStreamer`."""
    chunk: bytes = b''
    eof: bool = False


class BudgetExceeded(ValueError):
    """A wire or decoded byte budget stopped reception.

    Verified segments saved before this point stay available; the caller
    decides whether to resume from the stored cursor.
    """

    def __init__(self, budget, detail):
        super().__init__(f'{budget}: {detail}')
        self.budget=budget
        self.detail=detail


class WireStreamer:
    """Read and decompress an HTTP response body incrementally under budgets.

    Unlike :func:`fetch`, this never blocks for the whole entity.  ``read``
    returns as soon as one network read yields bytes, so callers can publish
    verified segments long before the server closes the connection.  Budgets
    are independent:

    ``max_wire_bytes``
        raw entity bytes before decompression; a larger ``Content-Length`` is
        rejected before the body is touched;
    ``max_decoded_bytes``
        inflated bytes handed back by ``read`` (independent of the wire bound,
        so a compressed body cannot expand past it unnoticed).  The record-size
        bound lives in the framing layer, where records are actually assembled.

    The bytes read before a budget stops reception remain in :attr:`raw` as
    evidence; :attr:`raw_truncated` records that more may have followed.
    """

    def __init__(self, url, *, method='GET', headers=None, body=None,
                 max_wire_bytes=1048576, max_decoded_bytes=4194304, timeout=5):
        for name, value in (('max_wire_bytes', max_wire_bytes),
                            ('max_decoded_bytes', max_decoded_bytes)):
            if value < 0:
                raise ValueError(f'{name} must be nonnegative')
        self.url=url
        self.max_wire_bytes=max_wire_bytes
        self.max_decoded_bytes=max_decoded_bytes
        self.timeout=timeout
        self.raw=bytearray()
        self.wire_bytes=0
        self.decoded_bytes=0
        self.raw_truncated=False
        self.limit=None
        self.stream=None
        self.response=None
        self._at_eof=False
        self._finish_data=b''
        self._budget_pending=False
        request=urllib.request.Request(
            url, data=body, headers=headers or {}, method=method)
        opener=urllib.request.build_opener(NoRedirect)
        try:
            stream=opener.open(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            stream=exc
        self.stream=stream
        headers_=tuple(stream.headers.items())
        self.response=Response(url, stream.status, headers_, b'')
        self.decoder=StreamDecoder(self.response.header('Content-Encoding'))

    def header(self, name, default=''):
        return self.response.header(name, default)

    @property
    def content_encoding(self):
        return self.response.header('Content-Encoding')

    @property
    def eof(self):
        return self._at_eof

    def read(self):
        if self._budget_pending:
            raise BudgetExceeded(
                'decoded-bytes',
                f'decoded body exceeds {self.max_decoded_bytes} bytes')
        if self._finish_data:
            data, self._finish_data = self._finish_data, b''
            return self._deliver(data)
        if self._at_eof:
            return WireRead(eof=True)
        length_header=self.response.header('Content-Length')
        if length_header.isdigit() and not self.raw \
                and int(length_header) > self.max_wire_bytes:
            self.limit='wire-bytes'
            raise BudgetExceeded(
                'wire-bytes',
                f'Content-Length {length_header} exceeds {self.max_wire_bytes}')
        try:
            chunk=self.stream.read1(65536)
        except Exception as exc:  # timeouts, resets, premature close
            self.close()
            raise ConnectionError(str(exc)) from exc
        if not chunk:
            self._at_eof=True
            try:
                self._finish_data=self.decoder.finish()
            except DecodeError:
                self.close()
                raise
            if self._finish_data:
                return self._deliver(b'')
            self.close()
            return WireRead(eof=True)
        self.raw.extend(chunk)
        self.wire_bytes+=len(chunk)
        if self.wire_bytes > self.max_wire_bytes:
            self.raw_truncated=True
            del self.raw[self.max_wire_bytes:]
            self.limit='wire-bytes'
            raise BudgetExceeded(
                'wire-bytes', f'raw body exceeds {self.max_wire_bytes} bytes')
        try:
            data=self.decoder.feed(chunk)
        except DecodeError:
            self.close()
            raise
        if len(data) > self.max_decoded_bytes-self.decoded_bytes:
            # A gzip bomb must not inflate past the budget.  Bytes inside the
            # budget are still handed over so a segment ending exactly at the
            # limit can verify; the next read raises.
            allowed=self.max_decoded_bytes-self.decoded_bytes
            self.decoded_bytes=self.max_decoded_bytes
            self.raw_truncated=True
            self.limit='decoded-bytes'
            self._budget_pending=True
            return WireRead(chunk=bytes(data[:allowed]))
        return self._deliver(data)

    def _deliver(self, data):
        if self.decoded_bytes+len(data) > self.max_decoded_bytes:
            self.decoded_bytes=self.max_decoded_bytes
            self.raw_truncated=True
            self.limit='decoded-bytes'
            raise BudgetExceeded(
                'decoded-bytes',
                f'decoded body exceeds {self.max_decoded_bytes} bytes')
        self.decoded_bytes+=len(data)
        return WireRead(chunk=bytes(data))

    def close(self):
        stream, self.stream = self.stream, None
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False
