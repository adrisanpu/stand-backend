#!/usr/bin/env python3
"""Dependency-free example client. Smoke mode performs only two authenticated reads."""
import argparse
import json
import os
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

class KivlyError(Exception):
    def __init__(self, status, request_id=None):
        self.status = status
        self.request_id = request_id
        super().__init__(f'Kivly HTTP {status}; request_id={request_id or "unavailable"}')

class NoRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

class KivlyClient:
    def __init__(self, client_id, client_secret, base_url='https://api.getkivly.com', timeout=30):
        url = urllib.parse.urlsplit(base_url)
        if url.scheme != 'https' or not url.netloc or url.username or url.password or url.query or url.fragment or url.path not in ('','/'):
            raise ValueError('base_url must be an HTTPS origin without credentials')
        if not client_id or not client_secret:
            raise ValueError('Client credentials are required')
        self.base_url = base_url.rstrip('/')
        self.client_id, self.client_secret = client_id, client_secret
        self.timeout = timeout
        self.token, self.expires_at = None, 0
        self.lock = threading.Lock()
        self.opener = urllib.request.build_opener(NoRedirects())

    def _send(self, method, path, body=None, token=None):
        headers = {'Accept':'application/json','User-Agent':'Kivly-Integration-Example/1.1'}
        if token:
            headers['Authorization'] = 'Bearer ' + token
        data = None
        if body is not None:
            headers['Content-Type'] = 'application/json'
            data = json.dumps(body).encode()
        req = urllib.request.Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            response = self.opener.open(req, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            return response.status, response.headers, response.read()

    def _access_token(self):
        with self.lock:
            if self.token and time.monotonic() < self.expires_at:
                return self.token
            status, headers, raw = self._send('POST','/api/v1/auth/token',{'client_id': self.client_id,'client_secret':self.client_secret})
            if status != 200:
                raise KivlyError(status, headers.get('X-Request-ID') or headers.get('CF-Ray'))
            data = json.loads(raw)['data']
            self.token = data['token']
            self.expires_at = time.monotonic() + max(0, int(data['expires_in'])-30)
            return self.token

    def request(self, method, path, body=None):
        method = method.upper()
        parsed = urllib.parse.urlsplit(path)
        if not path.startswith('/api/v1/') or parsed.scheme or parsed.netloc or parsed.fragment:
            raise ValueError('Use an API path under /api/v1/, not an absolute URL')
        token = self._access_token()
        safe = method in ('GET','HEAD','OPTIONS')
        for attempt in range(3):
            status, headers, raw = self._send(method,path,body,token)
            if 200 <= status < 300:
                return json.loads(raw) if raw and 'application/json' in headers.get('Content-Type','') else raw
            if status == 401 and safe and attempt == 0:
                self.expires_at = 0
                token = self._access_token()
                continue
            if status in (429,503) and safe and attempt < 2:
                try:
                    delay = max(1, int(headers.get('Retry-After','1')))
                except ValueError:
                    delay = 1
                if delay <= 60:
                    time.sleep(delay + random.random())
                    continue
            raise KivlyError(status, headers.get('X-Request-ID') or headers.get('CF-Ray'))
        raise KivlyError(status)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--smoke',action='store_true',help='Read integration context and branch count; makes live requests')
    args = parser.parse_args()
    if not args.smoke:
        parser.print_help()
        return
    client = KivlyClient(os.environ.get('KIVLY_CLIENT_ID'), os.environ.get('KIVLY_CLIENT_SECRET'), os.environ.get('KIVLY_BASE_URL','https://api.getkivly.com'))
    context = client.request('GET','/api/v1/integration/context')
    branches = client.request('GET','/api/v1/branches')
    print(json.dumps({'connected':True,'location_id':context['location_id'],'branch_count':len(branches),
                      'requests_per_minute':context['requests_per_minute']}))

if __name__ == '__main__':
    main()
