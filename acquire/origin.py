"""可配置的本地 HTTP 来源；按请求条件返回合成响应并保存实际请求日志。"""
import argparse
import base64
import json
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
            self.send_response(response.get('status',200))
            for k,v in response.get('headers',{}).items():
                self.send_header(k,v)
            self.send_header('Content-Length',str(len(body)))
            self.end_headers()
            self.wfile.write(body)
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
