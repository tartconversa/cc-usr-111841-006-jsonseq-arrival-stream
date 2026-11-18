import json
import sqlite3
import time

STREAM_SCHEMA = """
CREATE TABLE IF NOT EXISTS stream_attempts(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    request_url TEXT NOT NULL,
    from_segment TEXT,
    status INTEGER,
    outcome TEXT NOT NULL DEFAULT 'running',
    reason TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT '',
    content_encoding TEXT NOT NULL DEFAULT '',
    wire_bytes INTEGER NOT NULL DEFAULT 0,
    decoded_bytes INTEGER NOT NULL DEFAULT 0,
    raw_truncated INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    raw BLOB NOT NULL DEFAULT x''
);
CREATE TABLE IF NOT EXISTS stream_segments(
    segment TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    verified INTEGER NOT NULL DEFAULT 0,
    complete INTEGER NOT NULL DEFAULT 0,
    empty INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL,
    request_url TEXT NOT NULL,
    attempt_id INTEGER NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    declared_count INTEGER,
    expected_sha256 TEXT,
    actual_sha256 TEXT,
    next_segment TEXT,
    seq_contiguous INTEGER NOT NULL DEFAULT 1,
    decoded_start INTEGER,
    decoded_end INTEGER,
    corruption_offset INTEGER,
    corruption_reason TEXT,
    open_record_offset INTEGER,
    open_record_length INTEGER,
    reasons TEXT NOT NULL DEFAULT '[]',
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_events(
    segment TEXT NOT NULL,
    position INTEGER NOT NULL,
    seq INTEGER,
    event_id TEXT,
    decoded_offset INTEGER,
    raw BLOB NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY(segment, position)
);
CREATE TABLE IF NOT EXISTS stream_corruptions(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id INTEGER NOT NULL,
    segment TEXT,
    offset INTEGER,
    length INTEGER,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    raw BLOB NOT NULL DEFAULT x''
);
CREATE TABLE IF NOT EXISTS stream_cursor(
    source TEXT PRIMARY KEY,
    next_segment TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS stream_events_seq ON stream_events(segment, seq);
CREATE INDEX IF NOT EXISTS stream_corruptions_segment ON stream_corruptions(segment, attempt_id);
"""

SEGMENT_COLUMNS = (
    'segment,status,verified,complete,empty,source,request_url,attempt_id,'
    'event_count,declared_count,expected_sha256,actual_sha256,next_segment,'
    'seq_contiguous,decoded_start,decoded_end,corruption_offset,corruption_reason,'
    'open_record_offset,open_record_length,reasons,first_seen_at,updated_at')


def _now():
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())


class Store:
    def __init__(self, path):
        self.connection=sqlite3.connect(path)
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS documents(key TEXT PRIMARY KEY,value TEXT NOT NULL,receipt TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,url TEXT NOT NULL,status INTEGER NOT NULL,headers TEXT NOT NULL,body BLOB NOT NULL);
        """)
        self.connection.executescript(STREAM_SCHEMA)

    def save_response(self, receipt, response, rows):
        if len({r['key'] for r in rows}) != len(rows):
            raise ValueError('duplicate keys in response')
        with self.connection:
            self.connection.execute('INSERT OR IGNORE INTO receipts VALUES(?,?,?,?,?)',
                (receipt,response.url,response.status,json.dumps(response.headers),response.body))
            for row in rows:
                self.connection.execute('INSERT OR REPLACE INTO documents VALUES(?,?,?)',
                    (row['key'],json.dumps(row['value'],ensure_ascii=False),receipt))

    def documents(self):
        return [dict(key=k,value=json.loads(v),receipt=r) for k,v,r in self.connection.execute('SELECT key,value,receipt FROM documents ORDER BY key')]

    def receipt(self, identifier):
        row=self.connection.execute('SELECT url,status,headers,body FROM receipts WHERE id=?',(identifier,)).fetchone()
        return None if row is None else dict(url=row[0],status=row[1],headers=json.loads(row[2]),body=row[3])

    # ---- streaming reception -------------------------------------------------

    def start_stream_attempt(self, source, request_url, from_segment, *,
                             content_encoding=''):
        """Record the start of a reception.

        The cursor is created only when no cursor exists yet.  A cursor that
        already points at an unfinished segment is left untouched, so a caller
        that reconnects with a stale ``from`` cannot skip unverified work.
        """
        now=_now()
        with self.connection:
            if from_segment is not None:
                self.connection.execute(
                    'INSERT OR IGNORE INTO stream_cursor VALUES(?,?,?)',
                    (source,from_segment,now))
            cursor=self.connection.execute(
                'INSERT INTO stream_attempts(source,request_url,from_segment,'
                'content_encoding,started_at) VALUES(?,?,?,?,?)',
                (source,request_url,from_segment,content_encoding,now))
            return cursor.lastrowid

    def finish_stream_attempt(self, attempt_id, *, status, outcome, reason='',
                              detail='', wire_bytes=0, decoded_bytes=0, raw=b'',
                              raw_truncated=False):
        with self.connection:
            self.connection.execute(
                'UPDATE stream_attempts SET status=?,outcome=?,reason=?,detail=?,'
                'wire_bytes=?,decoded_bytes=?,raw_truncated=?,finished_at=?,raw=? '
                'WHERE id=?',
                (status,outcome,reason,detail,wire_bytes,decoded_bytes,
                 1 if raw_truncated else 0,_now(),sqlite3.Binary(raw),attempt_id))

    def save_stream_segment(self, segment, events, attempt_id, *, final):
        """Persist one observation of a segment atomically.

        A damaged or unfinished replay never downgrades an already verified
        segment: verified segments are write-once, and their stored events are
        kept.  The cursor advances only when a segment newly verifies.
        ``final`` marks the segment's terminal observation (an end record or
        the end of the reception); checkpoint writes keep ``partial`` state
        visible while a long segment is still arriving.
        """
        now=_now()
        existing=self.connection.execute(
            'SELECT status FROM stream_segments WHERE segment=?',
            (segment['segment'],)).fetchone()
        if existing is not None and existing[0]=='verified' and segment['status']!='verified':
            return 'verified'
        first_seen=now
        if existing is not None:
            first_seen=self.connection.execute(
                'SELECT first_seen_at FROM stream_segments WHERE segment=?',
                (segment['segment'],)).fetchone()[0]
        open_record=segment.get('open_record') or {}
        with self.connection:
            self.connection.execute(
                'INSERT OR REPLACE INTO stream_segments('+SEGMENT_COLUMNS+') '
                'VALUES('+','.join('?'*23)+')',
                (segment['segment'],segment['status'],
                 1 if segment.get('verified') else 0,
                 1 if segment.get('complete') else 0,
                 1 if segment.get('empty') else 0,
                 segment['source'],segment['request_url'],attempt_id,
                 segment['event_count'],segment.get('declared_count'),
                 segment.get('expected_sha256'),segment.get('actual_sha256'),
                 segment.get('next_segment'),
                 1 if segment.get('seq_contiguous',True) else 0,
                 segment.get('decoded_start'),segment.get('decoded_end'),
                 segment.get('corruption_offset'),segment.get('corruption_reason'),
                 open_record.get('offset'),open_record.get('length'),
                 json.dumps(segment.get('reasons',[]),ensure_ascii=False),
                 first_seen,now))
            if final:
                self.connection.execute(
                    'DELETE FROM stream_events WHERE segment=?',(segment['segment'],))
            for event in events:
                self.connection.execute(
                    'INSERT OR REPLACE INTO stream_events('
                    'segment,position,seq,event_id,decoded_offset,raw,value) '
                    'VALUES(?,?,?,?,?,?,?)',
                    (segment['segment'],event['position'],event.get('seq'),
                     event.get('event_id'),event.get('offset'),
                     sqlite3.Binary(event['raw']),
                     json.dumps(event['value'],ensure_ascii=False)))
            if final and segment['status']=='verified' \
                    and (existing is None or existing[0]!='verified'):
                next_segment=segment.get('next_segment')
                if next_segment:
                    self.connection.execute(
                        'UPDATE stream_cursor SET next_segment=?,updated_at=? '
                        'WHERE source=?',
                        (next_segment,now,segment['source']))
        return segment['status']

    def add_stream_corruption(self, attempt_id, segment, item):
        with self.connection:
            self.connection.execute(
                'INSERT INTO stream_corruptions(attempt_id,segment,offset,length,'
                'reason,detail,raw) VALUES(?,?,?,?,?,?,?)',
                (attempt_id,segment,item.get('offset'),item.get('length'),
                 item['reason'],item.get('detail',''),
                 sqlite3.Binary(item.get('raw') or b'')))

    def stream_cursor(self, source):
        row=self.connection.execute(
            'SELECT next_segment FROM stream_cursor WHERE source=?',(source,)).fetchone()
        return None if row is None else row[0]

    def stream_segments(self, source=None):
        query='SELECT '+SEGMENT_COLUMNS+' FROM stream_segments'
        args=()
        if source is not None:
            query+=' WHERE source=?'
            args=(source,)
        # Source order: oldest first_seen first, then decoded position, so
        # callers can read a source's segments back in stream order.
        query+=' ORDER BY first_seen_at, decoded_start, segment'
        return [self._segment_row(row)
                for row in self.connection.execute(query,args)]

    def stream_segment(self, identifier):
        row=self.connection.execute(
            'SELECT '+SEGMENT_COLUMNS+' FROM stream_segments WHERE segment=?',
            (identifier,)).fetchone()
        if row is None:
            return None
        segment=self._segment_row(row)
        segment['records']=[dict(
            position=p,seq=s,event_id=i,offset=o,value=json.loads(v),raw=r)
            for p,s,i,o,v,r in self.connection.execute(
                'SELECT position,seq,event_id,decoded_offset,value,raw '
                'FROM stream_events WHERE segment=? ORDER BY position',
                (identifier,))]
        segment['corruptions']=[self._corruption_row(c) for c in self.connection.execute(
            'SELECT id,attempt_id,segment,offset,length,reason,detail,raw '
            'FROM stream_corruptions WHERE segment=? ORDER BY offset,id',
            (identifier,))]
        segment['attempt']=self.stream_evidence(segment['attempt_id'], include_raw=False)
        return segment

    @staticmethod
    def _segment_row(row):
        segment=dict(
            segment=row[0],status=row[1],verified=bool(row[2]),
            complete=bool(row[3]),empty=bool(row[4]),source=row[5],
            request_url=row[6],attempt_id=row[7],event_count=row[8],
            declared_count=row[9],expected_sha256=row[10],
            actual_sha256=row[11],next_segment=row[12],
            seq_contiguous=bool(row[13]),decoded_start=row[14],
            decoded_end=row[15],corruption_offset=row[16],
            corruption_reason=row[17],reasons=json.loads(row[20]),
            first_seen_at=row[21],updated_at=row[22])
        # Explicit decoded-byte range of this segment's source text; the open
        # range marks an RS record that never received its LF terminator.
        segment['source_range']=dict(
            start=segment['decoded_start'],end=segment['decoded_end'])
        segment['open_record']=(None if row[18] is None else
                                dict(offset=row[18],length=row[19]))
        # Three outcomes stay distinct: a verified segment may legitimately
        # hold zero events, while corrupt/unfinished segments never do.
        if segment['status']=='verified':
            segment['integrity']='verified'
        elif segment['status']=='corrupt':
            segment['integrity']='corrupt'
        else:
            segment['integrity']='unfinished'
        return segment

    @staticmethod
    def _corruption_row(row):
        return dict(id=row[0],attempt_id=row[1],segment=row[2],offset=row[3],
                    length=row[4],reason=row[5],detail=row[6],raw=row[7])

    def stream_attempts(self, source=None):
        query=('SELECT id,source,request_url,from_segment,status,outcome,reason,detail,'
               'content_encoding,wire_bytes,decoded_bytes,raw_truncated,started_at,'
               'finished_at,length(raw) FROM stream_attempts')
        args=()
        if source is not None:
            query+=' WHERE source=?'
            args=(source,)
        query+=' ORDER BY id'
        return [dict(id=r[0],source=r[1],request_url=r[2],from_segment=r[3],
                     status=r[4],outcome=r[5],reason=r[6],detail=r[7],
                     content_encoding=r[8],wire_bytes=r[9],decoded_bytes=r[10],
                     raw_truncated=bool(r[11]),started_at=r[12],
                     finished_at=r[13],raw_size=r[14])
                for r in self.connection.execute(query,args)]

    def stream_evidence(self, attempt_id, *, include_raw=True):
        row=self.connection.execute(
            'SELECT id,source,request_url,from_segment,status,outcome,reason,detail,'
            'content_encoding,wire_bytes,decoded_bytes,raw_truncated,started_at,'
            'finished_at,raw FROM stream_attempts WHERE id=?',(attempt_id,)).fetchone()
        if row is None:
            return None
        evidence=dict(id=row[0],source=row[1],request_url=row[2],
                      from_segment=row[3],status=row[4],outcome=row[5],
                      reason=row[6],detail=row[7],content_encoding=row[8],
                      wire_bytes=row[9],decoded_bytes=row[10],
                      raw_truncated=bool(row[11]),started_at=row[12],
                      finished_at=row[13])
        evidence['corruptions']=[self._corruption_row(c) for c in self.connection.execute(
            'SELECT id,attempt_id,segment,offset,length,reason,detail,raw '
            'FROM stream_corruptions WHERE attempt_id=? ORDER BY offset,id',
            (attempt_id,))]
        if include_raw:
            evidence['raw']=row[14]
        else:
            evidence['raw_size']=len(row[14])
        return evidence

    def close(self):
        self.connection.close()
