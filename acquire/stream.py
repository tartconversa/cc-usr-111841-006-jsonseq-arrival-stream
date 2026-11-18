"""Incremental RFC 7464 reception with per-segment verification.

The receiver is deliberately transport-agnostic: :class:`StreamReceiver` consumes
decompressed bytes in arbitrary chunks and never waits for the connection to
close before publishing a segment whose end record verifies.  HTTP chunk
boundaries, UTF-8 code points and RS separators may fall anywhere; framing is
done on bytes, JSON is decoded only once a complete LF-terminated record exists.

Segment states (also stored in ``stream_segments.status``):

``verified``
    A syntactically valid ``end`` record matched begin/event records: count,
    per-record sequence and digest all agree.  A verified segment may contain
    zero events (``empty`` is then true) - it is still a real, complete segment.
``corrupt``
    At least one record in the segment failed framing, JSON, field, count,
    sequence or digest checks.  Records that did parse remain stored, but the
    segment is never complete and the cursor never advances to ``next``.
``partial``
    No end record has arrived and nothing is known to be damaged.  ``complete``
    is false, the cursor stays on this segment, and any bytes after the last RS
    that lack a closing LF are reported as the still-open range.
"""

import hashlib
import json


class StreamError(ValueError):
    """A reception problem that still leaves previously verified segments intact."""

    def __init__(self, reason, detail=''):
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


class StreamReceiver:
    """Frames, validates and persists RFC 7464 segments from decoded chunks."""

    def __init__(self, source, request_url='', *, store=None, attempt_id=None,
                 sample_bytes=256, max_open_record=1048576, checkpoint_events=32,
                 on_commit=None):
        if sample_bytes < 0:
            raise ValueError('sample_bytes must be nonnegative')
        if max_open_record <= 0:
            raise ValueError('max_open_record must be positive')
        self.source = source
        self.request_url = request_url
        self.store = store
        self.attempt_id = attempt_id
        self.sample_bytes = sample_bytes
        self.max_open_record = max_open_record
        self.checkpoint_events = checkpoint_events
        # Called as soon as a segment verifies, before the connection closes.
        self.on_commit = on_commit

        self.pending = bytearray()
        self.consumed = 0            # decoded offset of pending[0]
        self.decoded_bytes = 0       # total decoded bytes handed to the receiver
        self.last_offset = 0         # end of the last framed record or damage
        self.saved = []              # summaries saved during this reception
        self.open_offset = None      # trailing record that never got its LF
        self.open_length = None
        self.abandoned = False       # oversized record already reported
        self.finished = False
        self.final_open = None       # summary of the segment open at finish()

        self.segment = None
        self.events = []
        self.hasher = None
        self.raw_count = 0
        self.next_seq = 0
        self.seq_contiguous = True
        self.corrupt = False
        self.reasons = []
        self.start_offset = None
        self.end_offset = None
        self.next_segment = None
        self.declared_count = None
        self.expected_sha256 = None
        self.corruption_offset = None
        self.ended = False

    # -- byte framing ---------------------------------------------------------

    def feed(self, data):
        if self.finished:
            raise StreamError('already-finished', 'feed() after finish()')
        if not data:
            return
        self.decoded_bytes += len(data)
        self.pending.extend(data)
        self._drain()
        if len(self.pending) > self.max_open_record:
            self._damage(bytes(self.pending[:self.sample_bytes]), self.consumed,
                         'open-record-limit',
                         f'unterminated record exceeds {self.max_open_record} bytes')
            # Discard the oversized tail up to the next RS so a hostile record
            # cannot grow without bound; the damage already prevents the
            # segment from verifying, and later records keep arriving.
            self.abandoned = True
            self.last_offset = self.consumed + len(self.pending)
            self.pending.clear()
            self.consumed = self.decoded_bytes

    def finish(self):
        """Flush framing state once; an LF-less tail is reported, not accepted."""
        if self.finished:
            return
        self._drain()
        if self.pending:
            if self.pending[0] == 0x1e:
                self.open_offset = self.consumed + 1
                self.open_length = len(self.pending) - 1
            else:  # pragma: no cover - _drain always consumes RS-less bytes
                self._damage(bytes(self.pending), self.consumed,
                             'stray-bytes', f'{len(self.pending)} byte(s) outside any record')
                self.pending.clear()
        if self.segment is not None:
            self._close()
        else:
            self.final_open = None
        self.finished = True

    def _drain(self):
        while True:
            if self.abandoned:
                index = self.pending.find(b'\x1e')
                if index < 0:
                    self.consumed += len(self.pending)
                    self.last_offset = self.consumed
                    self.pending.clear()
                    return
                self.consumed += index
                self.last_offset = self.consumed
                del self.pending[:index]
                self.abandoned = False
            index = self.pending.find(b'\x1e')
            if index < 0:
                if self.pending:
                    self._damage(bytes(self.pending), self.consumed, 'stray-bytes',
                                 f'{len(self.pending)} byte(s) outside any record')
                    self.consumed += len(self.pending)
                    self.last_offset = self.consumed
                    self.pending.clear()
                return
            if index > 0:
                self._damage(bytes(self.pending[:index]), self.consumed, 'stray-bytes',
                             f'{index} byte(s) outside any record')
                self.last_offset = self.consumed + index
                self.consumed += index
                del self.pending[:index]
            end = self.pending.find(b'\n', 1)
            if end < 0:
                return
            payload = bytes(self.pending[1:end])
            offset = self.consumed + 1
            self.consumed += end + 1
            self.last_offset = self.consumed
            del self.pending[:end + 1]
            self._record(offset, payload)

    # -- record interpretation ------------------------------------------------

    def _record(self, offset, payload):
        try:
            value = json.loads(payload.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._damage(payload, offset, 'invalid-json', str(exc))
            return
        if not isinstance(value, dict):
            self._damage(payload, offset, 'invalid-record', 'record is not a JSON object')
            return
        kind = value.get('kind')
        if kind == 'begin':
            self._on_begin(value, offset)
        elif kind == 'event':
            self._on_event(value, offset, payload)
        elif kind == 'end':
            self._on_end(value, offset, payload)
        else:
            self._damage(payload, offset, 'unknown-kind', repr(kind))

    def _on_begin(self, value, offset):
        previous = None
        if self.segment is not None:
            # The open segment never received an end record; its parsed events
            # stay available but it cannot be declared complete.
            self._reason('end-record-missing', 'superseded by a new begin record',
                         offset)
            self._close()
            previous = self.saved[-1]
        segment = value.get('segment')
        if not isinstance(segment, str) or not segment:
            self._reason('invalid-begin', "'segment' must be a nonempty string",
                         offset)
            return
        self.segment = segment
        self.events = []
        self.hasher = hashlib.sha256()
        self.raw_count = 0
        self.next_seq = 0
        self.seq_contiguous = True
        self.corrupt = False
        self.reasons = []
        self.start_offset = offset
        self.end_offset = None
        self.next_segment = None
        self.declared_count = None
        self.expected_sha256 = None
        self.corruption_offset = None
        self.ended = False
        self.open_offset = None
        self.open_length = None
        if previous is not None and previous['segment'] == segment                 and not previous['verified']:
            # A replay of a segment already damaged in this same connection
            # cannot turn it complete; the earlier damage stays attached.
            self.corrupt = True
            self.reasons = [dict(item) for item in previous['reasons']]
            self.corruption_offset = previous['corruption_offset']

    def _on_event(self, value, offset, payload):
        if self.segment is None:
            self._persist_corruption(offset, len(payload), 'event-without-begin',
                                     payload[:self.sample_bytes],
                                     'event record before any begin record')
            return
        if value.get('segment') != self.segment:
            self._reason('segment-mismatch',
                         f"event segment {value.get('segment')!r} != {self.segment!r}", offset)
        seq = value.get('seq')
        if not _is_int(seq):
            self._reason('invalid-seq', repr(seq), offset)
            seq = None
        elif seq != self.next_seq:
            self.seq_contiguous = False
            self._reason('seq-gap', f'expected {self.next_seq}, got {seq}', offset)
            self.next_seq = seq + 1
        else:
            self.next_seq = seq + 1
        self.hasher.update(payload)
        self.hasher.update(b'\n')
        self.raw_count += 1
        event_id = value.get('id')
        self.events.append(dict(
            position=len(self.events),
            seq=seq,
            event_id=str(event_id) if event_id is not None else None,
            offset=offset,
            raw=payload,
            value=value))
        if self.store is not None and len(self.events) % self.checkpoint_events == 0:
            self._save_snapshot(final=False)

    def _on_end(self, value, offset, payload):
        if self.segment is None:
            self._persist_corruption(
                offset, len(payload), 'end-without-begin', payload[:self.sample_bytes],
                f"end record for {value.get('segment')!r} with no open segment")
            return
        if value.get('segment') != self.segment:
            self._reason('segment-mismatch',
                         f"end segment {value.get('segment')!r} != {self.segment!r}", offset)
        declared_count = value.get('count')
        if not _is_int(declared_count) or declared_count < 0:
            self._reason('invalid-count', repr(declared_count), offset)
        else:
            self.declared_count = declared_count
            if declared_count != self.raw_count:
                self._reason('count-mismatch',
                             f'declared {declared_count}, observed {self.raw_count}', offset)
        expected = value.get('sha256')
        if not isinstance(expected, str):
            self._reason('invalid-digest', repr(expected), offset)
        else:
            self.expected_sha256 = expected.lower()
            actual = self.hasher.hexdigest()
            if self.expected_sha256 != actual:
                self._reason('digest-mismatch',
                             f'declared {self.expected_sha256}, computed {actual}', offset)
        next_segment = value.get('next')
        if next_segment is not None and (not isinstance(next_segment, str) or not next_segment):
            self._reason('invalid-next', repr(next_segment), offset)
        elif next_segment is not None:
            self.next_segment = next_segment
        self.ended = True
        self.end_offset = self.last_offset
        self._close()

    # -- evidence and state ---------------------------------------------------

    def _reason(self, reason, detail='', offset=None, *, persist=True):
        """Record an integrity failure against the open segment.

        Every reason is both kept in memory for the summary and, when a store
        is attached, persisted with the raw evidence sample so ``segment()``
        and ``evidence()`` can show where the stream was damaged.
        """
        self.corrupt = True
        if self.segment is not None:
            if self.corruption_offset is None:
                self.corruption_offset = offset
            self.reasons.append(dict(reason=reason, detail=detail, offset=offset))
        if persist:
            self._persist_corruption(offset, None, reason, b'', detail)

    def _damage(self, payload, offset, reason, detail):
        self._reason(reason, detail, offset, persist=False)
        self._persist_corruption(offset, len(payload), reason,
                                 payload[:self.sample_bytes], detail)

    def _persist_corruption(self, offset, length, reason, raw, detail=''):
        if self.store is None or self.attempt_id is None:
            return
        self.store.add_stream_corruption(self.attempt_id, self.segment, dict(
            offset=offset, length=length, reason=reason, detail=detail, raw=raw))

    def _summary(self):
        if self.ended and not self.corrupt:
            status = 'verified'
        elif self.corrupt:
            status = 'corrupt'
        else:
            status = 'partial'
        verified = status == 'verified'
        open_record = self.open_record()
        return dict(
            segment=self.segment,
            status=status,
            verified=verified,
            complete=verified,
            empty=verified and self.raw_count == 0,
            source=self.source,
            request_url=self.request_url,
            event_count=len(self.events),
            declared_count=self.declared_count,
            expected_sha256=self.expected_sha256,
            actual_sha256=self.hasher.hexdigest(),
            next_segment=self.next_segment,
            seq_contiguous=self.seq_contiguous,
            decoded_start=self.start_offset,
            # The last byte accounted for by this segment: its end record
            # when closed, otherwise framed records plus any open range.
            decoded_end=(self.end_offset if self.ended else self.last_offset),
            corruption_offset=self.corruption_offset,
            corruption_reason=(self.reasons[0]['reason'] if self.reasons else None),
            open_record=open_record,
            reasons=[dict(item) for item in self.reasons],
        )

    def open_record(self):
        """Decoded range after the last RS that has no LF yet, if any.

        Reported for the segment currently being assembled and for a trailing
        record that was abandoned at end of input.  The range starts just after
        the RS so the RS itself is not counted as unverifiable content.
        """
        if self.segment is not None and not self.ended \
                and self.pending.startswith(b'\x1e'):
            return dict(offset=self.consumed + 1, length=len(self.pending) - 1)
        if self.open_offset is not None:
            return dict(offset=self.open_offset, length=self.open_length)
        return None

    def _save_snapshot(self, *, final):
        summary = self._summary()
        if self.store is not None:
            self.store.save_stream_segment(
                summary, self.events, self.attempt_id, final=final)
        self.saved.append(summary)
        if final and summary['status'] == 'verified' and self.on_commit is not None:
            self.on_commit(summary)

    def _close(self):
        self._save_snapshot(final=True)
        self.final_open = self.saved[-1]
        self.segment = None
        self.events = []
        self.reasons = []
        self.hasher = None
        self.corrupt = False
        self.ended = False
        self.open_offset = None
        self.open_length = None
