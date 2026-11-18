"""可配置的本地 HTTP 来源；按请求条件返回合成响应并保存实际请求日志。

响应字段（除既有 status/headers/body/body_base64 外）：

``gzip``
    对整个实体做 gzip 压缩。
``split_at``
    按给定字节偏移把实体拆成多个 HTTP 分块写出（自动使用 chunked 传输，
    不发送 Content-Length），每块之后 flush。
``chunk_delay``
    每块之后等待的秒数，用于模拟连接持续打开、数据逐步到达。
``hold_open``
    最后一块之后、关闭连接之前等待的秒数。
"""
import argparse
import base64
import gzip
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from urllib.parse import parse_qs, urlsplit

def make_server(scenario, log=None):
    lock=Lock()
    visits={}
    events=[]
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*args):
            pass
        def do_GET(self):
            self.respond()
        def do_POST(self):
            self.respond()
        def respond(self):
            raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            entry=dict(method=self.command,path=self.path,headers=list(self.headers.items()),body_base64=base64.b64encode(raw).decode())
            with lock:
                events.append(entry)
                if log:
                    with open(log,'a',encoding='utf-8') as f:
                        f.write(json.dumps(entry,ensure_ascii=False)+'\n')
                matching=[]
                for index,route in enumerate(scenario['routes']):
                    if route.get('method','GET')!=self.command or route['path']!=self.path:
                        continue
                    if any(self.headers.get(k)!=v for k,v in route.get('require_headers',{}).items()):
                        continue
                    if 'body_contains' in route and route['body_contains'] not in raw.decode('utf-8','replace'):
                        continue
                    matching.append((index,route))
                if matching:
                    index,route=matching[0]
                    count=visits.get(index,0)
                    responses=route.get('responses',[route])
                    response=responses[min(count,len(responses)-1)]
                    visits[index]=count+1
                else:
                    response=dict(status=404,body='unmatched source request')
            body=base64.b64decode(response['body_base64']) if 'body_base64' in response else response.get('body','').encode('utf-8')
            headers=dict(response.get('headers',{}))
            if response.get('gzip'):
                body=gzip.compress(body)
                headers.setdefault('Content-Encoding','gzip')
            split=response.get('split_at')
            self.send_response(response.get('status',200))
            for k,v in headers.items():
                self.send_header(k,v)
            if split:
                self.send_header('Transfer-Encoding','chunked')
            else:
                self.send_header('Content-Length',str(len(body)))
            self.end_headers()
            delay=response.get('chunk_delay',0)
            if split:
                pieces=[]; start=0
                for cut in split:
                    pieces.append(body[start:cut]); start=cut
                pieces.append(body[start:])
                for piece in pieces:
                    self.wfile.write(f'{len(piece):x}\r\n'.encode()+piece+b'\r\n')
                    self.wfile.flush()
                    if delay:
                        time.sleep(delay)
                self.wfile.write(b'0\r\n\r\n')
                self.wfile.flush()
            else:
                self.wfile.write(body)
                self.wfile.flush()
            if response.get('hold_open'):
                time.sleep(response['hold_open'])
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    server.events=events
    server.visits=visits
    return server

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--scenario',default='fixtures/scenario.json')
    parser.add_argument('--log',default='requests.jsonl')
    args=parser.parse_args()
    server=make_server(json.loads(Path(args.scenario).read_text(encoding='utf-8')),args.log)
    print(f'http://127.0.0.1:{server.server_port}',flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()

if __name__=='__main__':
    main()
