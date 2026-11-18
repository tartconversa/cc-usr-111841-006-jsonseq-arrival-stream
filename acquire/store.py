import json
import sqlite3

class Store:
    def __init__(self, path):
        self.connection=sqlite3.connect(path)
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS documents(key TEXT PRIMARY KEY,value TEXT NOT NULL,receipt TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS receipts(id TEXT PRIMARY KEY,url TEXT NOT NULL,status INTEGER NOT NULL,headers TEXT NOT NULL,body BLOB NOT NULL);
        """)

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

    def close(self):
        self.connection.close()
