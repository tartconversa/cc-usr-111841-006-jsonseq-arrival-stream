import base64
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest

from acquire.origin import make_server
from acquire.service import (
    AcquisitionCancelled, stream_collect, stream_show)
from acquire.stream import StreamReceiver
from acquire.store import Store
from acquire.transport import BudgetExceeded


def event(segment, seq, identifier, **fields):
    value = dict(kind='event', segment=segment, seq=seq, id=identifier)
    value.update(fields)
    return value


def wire(*records):
    """Serialize records as RFC 7464 (RS + JSON + LF)."""
    out = bytearray()
    for record in records:
        raw = record if isinstance(record, bytes) else json.dumps(
            record, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        out += b'\x1e' + raw + b'\n'
    return bytes(out)


def event_texts(records):
    return [r if isinstance(r, bytes) else json.dumps(
        r, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        for r in records]


def segment_records(segment, events, *, next_segment=None, count='auto',
                    sha='auto', sequence='auto'):
    begin = dict(kind='begin', segment=segment)
    texts = event_texts(events)
    body = list(events)
    digest = hashlib.sha256(b''.join(t + b'\n' for t in texts)).hexdigest()
    end = dict(kind='end', segment=segment,
               count=len(events) if count == 'auto' else count,
               sha256=digest if sha == 'auto' else sha)
    if next_segment is not None:
        end['next'] = next_segment
    return [begin] + body + [end]


class StreamReceiverTests(unittest.TestCase):
    def receiver(self, **kwargs):
        return StreamReceiver('test-source', **kwargs)

    def feed_in_chunks(self, receiver, data, size):
        for start in range(0, len(data), size):
            receiver.feed(data[start:start + size])
        receiver.finish()

    def test_byte_at_a_time_framing_keeps_multibyte_and_rs_intact(self):
        records = segment_records(
            's1', [event('s1', 0, 'a1', hall='东厅'), event('s1', 1, 'a2')],
            next_segment='s2')
        receiver = self.receiver()
        self.feed_in_chunks(receiver, wire(*records), 1)
        summary = receiver.saved[-1]
        self.assertEqual(summary['status'], 'verified')
        self.assertEqual(summary['event_count'], 2)

    def test_verified_segment_is_delivered_before_finish(self):
        records = segment_records('s1', [event('s1', 0, 'a1')], next_segment='s2')
        receiver = self.receiver()
        receiver.feed(wire(*records))
        # No finish(): the end record alone must be sufficient.
        self.assertEqual(receiver.saved[-1]['status'], 'verified')
        self.assertEqual(receiver.saved[-1]['next_segment'], 's2')
        receiver.finish()

    def test_trailing_incomplete_record_is_reported_not_certified(self):
        data = wire(*segment_records('s1', [event('s1', 0, 'a1')], next_segment='s2'))
        data += b'\x1e{"kind":"begin","segment":"s2"}\n\x1e{"kind":"event","segment":"s2"'
        receiver = self.receiver()
        receiver.feed(data)
        receiver.finish()
        self.assertEqual(receiver.saved[0]['status'], 'verified')
        second = receiver.saved[-1]
        self.assertEqual(second['status'], 'partial')
        self.assertFalse(second['complete'])
        self.assertIsNotNone(second['open_record'])
        self.assertEqual(second['open_record']['offset'],
                         data.rindex(b'\x1e') + 1)

    def test_corrupt_record_does_not_lose_following_segments(self):
        data = wire(
            dict(kind='begin', segment='s1'),
            b'{"kind":"event","segment":"s1","seq":0,',  # truncated JSON
            event('s1', 1, 'a2'),
            dict(kind='end', segment='s1', count=2,
                 sha256=hashlib.sha256(b''.join(
                     t + b'\n' for t in event_texts(
                         [event('s1', 1, 'a2')]))).hexdigest(),
                 next='s2'))
        data += wire(*segment_records('s2', [event('s2', 0, 'b1')],
                                      next_segment='s3'))
        receiver = self.receiver()
        self.feed_in_chunks(receiver, data, 7)
        first, second = receiver.saved
        self.assertEqual(first['status'], 'corrupt')
        self.assertFalse(first['verified'])
        self.assertEqual(first['event_count'], 1)
        self.assertEqual(first['corruption_reason'], 'invalid-json')
        self.assertEqual(second['status'], 'verified')
        self.assertEqual(second['segment'], 's2')

    def test_count_and_digest_must_match_declaration(self):
        records = segment_records('s1', [event('s1', 0, 'a1')], sha='0' * 64)
        receiver = self.receiver()
        receiver.feed(wire(*records))
        summary = receiver.saved[-1]
        self.assertEqual(summary['status'], 'corrupt')
        self.assertIn('digest-mismatch', [r['reason'] for r in summary['reasons']])
        self.assertEqual(summary['actual_sha256'],
                         hashlib.sha256(event_texts(
                             [event('s1', 0, 'a1')])[0] + b'\n').hexdigest())

    def test_sequence_gap_is_recorded(self):
        records = segment_records('s1', [event('s1', 0, 'a1'),
                                         event('s1', 2, 'a2')])
        receiver = self.receiver()
        receiver.feed(wire(*records))
        summary = receiver.saved[-1]
        self.assertEqual(summary['status'], 'corrupt')
        self.assertFalse(summary['seq_contiguous'])
        self.assertIn('seq-gap', [r['reason'] for r in summary['reasons']])

    def test_zero_event_segment_is_verified_and_marked_empty(self):
        receiver = self.receiver()
        receiver.feed(wire(*segment_records('s1', [], next_segment='s2')))
        receiver.finish()
        summary = receiver.saved[-1]
        self.assertEqual(summary['status'], 'verified')
        self.assertTrue(summary['empty'])
        self.assertTrue(summary['complete'])

    def test_open_record_limit_marks_damage_and_continues(self):
        receiver = self.receiver(max_open_record=64)
        # An oversized record arrives while s1 is open and is abandoned; the
        # segment must not verify even though a valid end record follows.
        receiver.feed(wire(dict(kind='begin', segment='s1'),
                           event('s1', 0, 'a1')) + b'\x1e' + b'x' * 200)
        receiver.feed(b'\n' + wire(*segment_records(
            's1', [event('s1', 0, 'a1')], next_segment='s2')))
        receiver.finish()
        self.assertEqual(receiver.saved[-1]['status'], 'corrupt')
        self.assertEqual(receiver.saved[-1]['segment'], 's1')
        self.assertEqual(receiver.saved[-1]['corruption_reason'],
                         'open-record-limit')

    def test_superseded_begin_keeps_previous_segment_but_not_complete(self):
        data = wire(dict(kind='begin', segment='s1'),
                    event('s1', 0, 'a1'),
                    dict(kind='begin', segment='s2'),
                    event('s2', 0, 'b1'),
                    dict(kind='end', segment='s2', count=1,
                         sha256=hashlib.sha256(
                             event_texts([event('s2', 0, 'b1')])[0] + b'\n'
                         ).hexdigest()))
        receiver = self.receiver()
        receiver.feed(data)
        receiver.finish()
        statuses = {s['segment']: s['status'] for s in receiver.saved}
        self.assertEqual(statuses['s1'], 'corrupt')
        # s1 is preserved with its parsed event but never complete; s2 starts
        # from a fresh begin and verifies on its own records.
        self.assertEqual(statuses['s2'], 'verified')
        self.assertEqual({s['segment']: s['event_count'] for s in receiver.saved},
                         {'s1': 1, 's2': 1})


class StreamServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory()
        self.database=str(Path(self.temporary.name)/'data.sqlite3')
        self.servers=[]
        self.threads=[]

    def tearDown(self):
        for server in self.servers:
            server.shutdown(); server.server_close()
        for thread in self.threads:
            thread.join()
        self.temporary.cleanup()

    def start(self, routes):
        server=make_server(dict(routes=routes))
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        self.servers.append(server); self.threads.append(thread)
        return f'http://127.0.0.1:{server.server_port}'

    def test_stream_saves_verified_segment_and_leaves_open_range(self):
        records=segment_records('s1',[event('s1',0,'a1',hall='东厅'),
                                      event('s1',1,'a2')],next_segment='s2')
        body=wire(*records)+wire(dict(kind='begin',segment='s2'))+\
            b'\x1e{"kind":"event","segment":"s2","seq":0'
        base=self.start([dict(path='/stream?from=s1',
                              gzip=True,split_at=[2,9,31,66,97,121],
                              body_base64=base64.b64encode(body).decode())])
        result=stream_collect(base+'/stream?from=s1',self.database,
                              max_wire_bytes=65536)
        self.assertEqual(result['outcome'],'incomplete')
        by_name={s['segment']:s for s in result['segments']}
        self.assertEqual(by_name['s1']['status'],'verified')
        self.assertEqual(by_name['s1']['source_range']['start'],1)
        self.assertGreater(by_name['s1']['source_range']['end'],1)
        self.assertEqual(by_name['s2']['status'],'partial')
        self.assertIsNotNone(by_name['s2']['open_record'])
        self.assertEqual(result['cursor'],'s2')

        detail=stream_show(self.database,segment='s1')
        self.assertEqual([r['value']['id'] for r in detail['records']],['a1','a2'])
        self.assertEqual(detail['records'][0]['value']['hall'],'东厅')
        self.assertTrue(detail['records'][0]['raw'].startswith(b'{"kind":"event"'))
        evidence = stream_show(self.database, attempt=detail['attempt_id'])
        self.assertTrue(evidence['raw'].startswith(b'\x1f\x8b'))

    def test_verified_segment_arrives_while_connection_stays_open(self):
        records=segment_records('s1',[event('s1',0,'a1')],next_segment='s2')
        body=wire(*records)+wire(dict(kind='begin',segment='s2'))
        base=self.start([dict(path='/stream?from=s1',
                              body_base64=base64.b64encode(body).decode(),
                              split_at=[len(wire(*records))],
                              chunk_delay=1.0,hold_open=0)])
        seen={}
        started=time.monotonic()
        def on_segment(summary):
            seen[summary['segment']]=(summary['status'],time.monotonic()-started)
        result=stream_collect(base+'/stream?from=s1',self.database,
                              on_segment=on_segment,max_wire_bytes=65536,
                              timeout=10)
        self.assertEqual(seen['s1'][0],'verified')
        self.assertLess(seen['s1'][1],0.9)
        self.assertEqual(result['outcome'],'incomplete')

    def test_resume_uses_cursor_and_does_not_skip_unfinished_segment(self):
        complete=wire(*segment_records('s1',[event('s1',0,'a1')],next_segment='s2'))
        tail=wire(*segment_records('s2',[event('s2',0,'b1')],next_segment='s3'))
        base=self.start([dict(path='/stream?from=s1',
                              responses=[dict(body=complete.decode()),
                                         dict(body=(complete+tail).decode(),
                                              split_at=[len(complete)])]),
                         dict(path='/stream?from=s2',body=tail.decode())])
        first=stream_collect(base+'/stream?from=s1',self.database)
        self.assertEqual(first['cursor'],'s2')
        second=stream_collect(base+'/stream?from=s1',self.database)
        self.assertEqual(second['request_url'],base+'/stream?from=s2')
        self.assertEqual(second['from_segment'],'s2')
        self.assertEqual({s['segment']:s['status'] for s in second['segments']},
                         {'s1':'verified','s2':'verified'})
        self.assertEqual(second['cursor'],'s3')
        store=Store(self.database)
        try:
            self.assertEqual(store.stream_cursor(second['source']),'s3')
        finally:
            store.close()

    def test_stale_from_cannot_skip_verified_cursor(self):
        base=self.start([dict(path='/stream?from=s1',
                              body=wire(*segment_records(
                                  's1',[event('s1',0,'a1')],next_segment='s2')).decode()),
                         dict(path='/stream?from=s2',
                              body=wire(*segment_records(
                                  's2',[event('s2',0,'b1')],next_segment='s3')).decode())])
        first=stream_collect(base+'/stream?from=s1',self.database)
        self.assertEqual(first['from_segment'],'s1')
        second=stream_collect(base+'/stream?from=s1',self.database,
                              from_segment='s2')
        # The caller asked for s2, but the cursor advances only after s2
        # verifies, so replaying s2 is allowed and s1 remains present.
        self.assertEqual(second['from_segment'],'s2')
        self.assertEqual(second['cursor'],'s3')

    def test_budget_abort_keeps_verified_segments_and_records_evidence(self):
        records=segment_records('s1',[event('s1',0,'a1')],next_segment='s2')
        body=wire(*records)+wire(dict(kind='begin',segment='s2'))+b'x'*500
        base=self.start([dict(path='/stream?from=s1',
                              body_base64=base64.b64encode(body).decode(),
                              split_at=[len(wire(*records)),
                                        len(wire(*records))+len(
                                            wire(dict(kind='begin',segment='s2')))])])
        with self.assertRaises(BudgetExceeded) as caught:
            stream_collect(base+'/stream?from=s1',self.database,
                           max_wire_bytes=len(wire(*records))+40)
        self.assertEqual(caught.exception.budget,'wire-bytes')
        segments=stream_show(self.database,source=base+'/stream?from=s1')
        by_name={s['segment']:s for s in segments['segments']}
        self.assertEqual(by_name['s1']['status'],'verified')
        self.assertEqual(segments['cursor'],'s2')
        attempt=segments['attempts'][-1]
        self.assertEqual(attempt['outcome'],'aborted')
        self.assertEqual(attempt['reason'],'wire-bytes')
        evidence=stream_show(self.database,attempt=attempt['id'])
        self.assertTrue(evidence['raw'])
        self.assertTrue(evidence['raw_truncated'])

    def test_decoded_budget_is_separate_from_wire_budget(self):
        filler='x'*4000
        records=segment_records('s1',[event('s1',0,'a1',note=filler)],
                                next_segment='s2')
        body=wire(*records)
        base=self.start([dict(path='/stream?from=s1',gzip=True,
                              body_base64=base64.b64encode(body).decode())])
        with self.assertRaises(BudgetExceeded) as caught:
            stream_collect(base+'/stream?from=s1',self.database,
                           max_wire_bytes=len(body)+100,
                           max_decoded_bytes=256)
        self.assertEqual(caught.exception.budget,'decoded-bytes')
        self.assertEqual(stream_show(self.database,
                                     source=base+'/stream?from=s1')['cursor'],
                         's1')

    def test_cancel_keeps_verified_segment_and_open_range(self):
        records=segment_records('s1',[event('s1',0,'a1')],next_segment='s2')
        body=wire(*records)+wire(dict(kind='begin',segment='s2'))
        base=self.start([dict(path='/stream?from=s1',
                              body_base64=base64.b64encode(body).decode(),
                              split_at=[len(wire(*records))],
                              chunk_delay=0.05)])
        checks={'count':0}
        def should_stop():
            checks['count']+=1
            return checks['count']>2
        with self.assertRaises(AcquisitionCancelled):
            stream_collect(base+'/stream?from=s1',self.database,
                           should_stop=should_stop,timeout=10,
                           max_wire_bytes=65536)
        state=stream_show(self.database,source=base+'/stream?from=s1')
        by_name={s['segment']:s for s in state['segments']}
        self.assertEqual(by_name['s1']['status'],'verified')
        self.assertEqual(by_name['s2']['status'],'partial')
        self.assertEqual(state['cursor'],'s2')
        self.assertEqual(state['attempts'][-1]['outcome'],'aborted')
        self.assertEqual(state['attempts'][-1]['reason'],'cancelled')

    def test_truncated_connection_does_not_certify_last_event(self):
        full=wire(*segment_records('s1',[event('s1',0,'a1')],next_segment='s2'))
        cut=full[:-1]
        base=self.start([dict(path='/stream?from=s1',
                              body_base64=base64.b64encode(cut).decode())])
        result=stream_collect(base+'/stream?from=s1',self.database)
        self.assertEqual(result['outcome'],'incomplete')
        segment=result['segments'][-1]
        self.assertEqual(segment['status'],'partial')
        self.assertFalse(segment['verified'])
        self.assertFalse(segment['complete'])
        self.assertEqual(segment['event_count'],1)
        self.assertEqual(segment['open_record']['offset'],cut.rindex(b'\x1e')+1)

    def test_corruption_keeps_following_segment_available(self):
        broken=wire(dict(kind='begin',segment='s1'))+b'\x1e{not json]\n'\
            +wire(event('s1',0,'a1'),
                   dict(kind='end',segment='s1',count=1,
                        sha256=hashlib.sha256(
                            event_texts([event('s1',0,'a1')])[0]+b'\n').hexdigest(),
                        next='s2'))
        body=broken+wire(*segment_records('s2',[event('s2',0,'b1')],
                                          next_segment='s3'))
        base=self.start([dict(path='/stream?from=s1',
                              body_base64=base64.b64encode(body).decode(),
                              split_at=[1,len(broken)])])
        result=stream_collect(base+'/stream?from=s1',self.database,
                              max_wire_bytes=65536)
        by_name={s['segment']:s for s in result['segments']}
        self.assertEqual(by_name['s1']['status'],'corrupt')
        self.assertNotEqual(by_name['s1']['integrity'],'verified')
        self.assertEqual(by_name['s2']['status'],'verified')
        detail=stream_show(self.database,segment='s1')
        self.assertEqual(detail['corruptions'][0]['reason'],'invalid-json')
        self.assertIn(b'not json', detail['corruptions'][0]['raw'])
        self.assertEqual(result['cursor'],'s3')
        self.assertEqual(result['outcome'],'complete')

    def test_gzip_corruption_is_reported_without_losing_store(self):
        base=self.start([dict(path='/stream?from=s1',
                              body_base64=base64.b64encode(b'not gzip at all').decode(),
                              headers={'Content-Encoding':'gzip'})])
        with self.assertRaises(ValueError):
            stream_collect(base+'/stream?from=s1',self.database)
        state=stream_show(self.database,source=base+'/stream?from=s1')
        self.assertEqual(state['attempts'][-1]['outcome'],'aborted')
        self.assertEqual(state['segments'],[])


if __name__ == '__main__':
    unittest.main()
