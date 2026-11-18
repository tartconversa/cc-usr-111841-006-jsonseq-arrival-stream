import argparse
import hashlib
import json
from .transport import fetch
from .decode import decode
from .store import Store

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

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('url')
    parser.add_argument('database')
    parser.add_argument('--method',default='GET')
    args=parser.parse_args()
    print(json.dumps(collect_one(args.url,args.database,method=args.method),ensure_ascii=False))

if __name__=='__main__':
    main()
