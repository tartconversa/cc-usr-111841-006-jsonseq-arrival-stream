from dataclasses import dataclass
import hashlib
import urllib.request
import urllib.error

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
