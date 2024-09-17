#!/usr/bin/env python3

import asyncio, traceback, re, logging
from asyncio.streams import StreamReader, StreamWriter
from contextlib import closing
from urllib.parse import parse_qs
from collections import namedtuple
from dataclasses import dataclass
import os.path


try:
    from itertools import batched   # Python 3.12
except ImportError:
    # Python < 3.12
    from itertools import islice
    def batched(iterable, n):
        if n < 1:
            raise ValueError('n must be at least one')
        it = iter(iterable)
        while batch := tuple(islice(it, n)):
            yield batch


# implementation idea from: https://github.com/aio-libs/async-timeout/blob/master/async_timeout/__init__.py
class async_timeout:
    def __init__(self, seconds=10):
        self._seconds = seconds
        self._cancelled = False
    async def __aenter__(self):
        loop = asyncio.get_running_loop()
        task = asyncio.current_task()
        self._killer = loop.call_at(loop.time() + self._seconds, self._cancel, task)
        return
    async def __aexit__(self, exc_type, exc, tb):
        if not self._killer.cancelled():
            self._killer.cancel()
        if self._cancelled:
            raise asyncio.TimeoutError
        # return True
    def _cancel(self, task):
        self._cancelled = True
        task.cancel()


@dataclass
class Request:
    '''Request'''
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    content_type: str
    content_length: int | None
    body: None | str | bytes
    remote_addr: str
    id: str


@dataclass
class Response:
    '''Response'''
    status: None | str
    headers: dict[str, str]
    body: None | str | bytes
    remote_addr: str
    id: str
    status_code: int = 0
    status_text: str = ''
    def get_status(self) -> str:
        if self.status:
            return self.status
        return f'{self.status_code} {self.status_text}'


class HeaderName(str):
    def __hash__(self):
        return hash(self.lower())
    def __eq__(self, other):
        return self.lower() == other.lower()

class Headers(dict):
    def __setitem__(self, key, value):
        super().__setitem__(HeaderName(key), value)


class AsyncServer:

    RouteHandler = namedtuple('RouteHandler', 'paths, methods, handler')
    _route_handlers = []
    _middleware_handlers = []
    next_handler_id = 1
    hostname = ''

    def __init__(self, log=logging.getLogger('server'), debug=False, hostname=''):
        self.log = log
        self.debug = debug
        self.hostname = hostname

    @staticmethod
    def _path_template_to_regex(template):
        parts = re.split(r'(\{[^}]+\})', template)
        regex = []
        for selected in batched(parts, 2):
            if len(selected) == 2:
                text, variable = selected
                regex.append(text)
                variable = variable.strip('{}')
                vtype = ''
                if ':' in variable:
                    variable, vtype = variable.split(':')
                if vtype == 'path':
                    regex.append(r'(?P<%s>.*)' % (variable,))
                else:
                    regex.append(r'(?P<%s>[^/]+)' % (variable,))
        regex.append(parts[-1].replace('*', '.*') + '$')
        return re.compile('^'+''.join(regex), re.I)

    def add_middleware(self, handler):
        self._middleware_handlers.append(handler)

    def middleware(self):
        def decorator(fn):
            self.add_middleware(fn)
            return fn
        return decorator

    def add_handler(self, methods, paths, handler):
        if type(methods) is str:
            methods = [methods]
        if type(paths) is str:
            paths = [paths]
        for i, path in enumerate(paths):
            paths[i] = self._path_template_to_regex(path)
        self._route_handlers.append(self.RouteHandler(paths=paths, methods=methods, handler=handler))

    def handle(self, methods, paths):
        def decorator(fn):
            self.add_handler(methods, paths, fn)
            return fn
        return decorator

    def get(self, paths):
        def decorator(fn):
            self.add_handler(['GET'], paths, fn)
            return fn
        return decorator

    def post(self, paths):
        def decorator(fn):
            self.add_handler(['POST'], paths, fn)
            return fn
        return decorator

    def put(self, paths):
        def decorator(fn):
            self.add_handler(['PUT'], paths, fn)
            return fn
        return decorator

    def delete(self, paths):
        def decorator(fn):
            self.add_handler(['DELETE'], paths, fn)
            return fn
        return decorator

    def _prepare_response(self, status, headers={}, body=None):
        if not body:
            body = b''
        elif body and type(body) is str:
            body = body.encode('utf8')
        # prepare header
        response = []
        response.append(f'HTTP/1.1 {status}')
        headers['Host'] = self.hostname
        headers['Content-Length'] = len(body) if body else 0
        for key, value in headers.items():
            response.append(f'{key}: {value}')
        response.append('')
        response.append('')
        return '\r\n'.join(response).encode('utf8') + body

    def _write_response(self, writer, response):
        if self.debug:
            self.log.info(f'[#{response.id}] Sending response to {response.remote_addr}: {response.get_status()}')
        data = self._prepare_response(response.get_status(), response.headers, response.body)
        writer.write(data)
        # if self.debug:
        #     self.log.debug(f'data sent to {addr}: {data}')

    @staticmethod
    def _default_headers(content_type=None):
        headers = {}
        if content_type is not None:
            headers['Content-Type'] = content_type
        return headers

    async def _call_handler(self, request):
        response = None
        # match handler
        for route_handler in self._route_handlers:
            if request.method not in route_handler.methods:
                continue
            for p in route_handler.paths:
                m = p.match(request.path)
                if not m:
                    continue
                # execute handler
                if asyncio.iscoroutine(route_handler.handler):
                    response = await route_handler.handler(request, **m.groupdict())
                else:
                    response = route_handler.handler(request, **m.groupdict())
                if response:
                    break
        if not response:
            return Response(404, 'Not found', self._default_headers(), id=request.id)
        if asyncio.iscoroutine(response):
            response = await response
        # if type(response) in (str, bytes):
        if type(response) is str:
            return Response(200, 'OK', self._default_headers('text'), id=request.id)
        if type(response) is bytes:
            return Response(200, 'OK', self._default_headers('application/octet-stream'), id=request.id)
        if isinstance(response, dict):
            return Response(**response, id=request.id, remote_addr=request.remote_addr)
        raise Exception('unknown response object: {!r}'.format(response))

    @staticmethod
    def _make_middleware_caller(middleware, call_next):
        async def middleware_caller(request):
            return await middleware(request, call_next)
        return middleware_caller

    async def _handle_request(self, request):
        call_next = self._call_handler
        for middleware in reversed(self._middleware_handlers):
            call_next = self._make_middleware_caller(middleware, call_next)
        return await call_next(request)

    async def _session(self, reader: StreamReader, writer: StreamWriter, timeout=30):
        remote_addr = ''
        handler_id = self.next_handler_id
        self.next_handler_id += 1
        log = self.log
        debug = self.debug
        if debug:
            log.debug(f'[#{handler_id}] Starting handler')
        try:
            async with async_timeout(timeout):
                try:
                    while True:
                        if debug:
                            log.debug(f'[#{handler_id}] Waiting for request')

                        data = await reader.readuntil(b'\r\n\r\n')
                        remote_addr = ':'.join(map(str, writer.get_extra_info('peername')))

                        if debug:
                            log.debug(f'Received {data} from {remote_addr!r}')

                        request = data.decode('utf8').split('\r\n')

                        request_line_parts = request[0].split(' ', 2)

                        if len(request_line_parts) != 3:
                            raise Exception('invalid request line: %s' % request[0])

                        method, path, http_ver = request_line_parts

                        if http_ver not in ('HTTP/1.0', 'HTTP/1.1'):
                            raise Exception('unknown HTTP version: %s' % http_ver)

                        expect_next_request = http_ver[-1] == '1'

                        if method not in ('GET', 'PUT', 'POST', 'DELETE', 'OPTIONS', 'PATCH', 'CONNECT'):
                            raise Exception('unknown method: %s' % method)

                        if not path.startswith('/'):
                            raise Exception('invalid path: %s' % path)

                        headers = Headers()
                        for line in request[1:]:
                            if not line:
                                continue
                            key, *value = map(str.strip, line.split(':', 1))
                            if len(value) != 1:
                                raise Exception('invalid header line: %s' % line)
                            headers[key] = value[0]    # do not process recurring headers

                        # hack: if hostname not set, get it from the request
                        if not self.hostname and headers.get('host'):
                            self.hostname = headers['host']

                        content_length = headers.get('Content-Length')
                        if content_length is not None:
                            content_length = int(content_length)
                        content_type = headers.get('Content-Type')

                        path, *qs = path.split('?', 1)

                        if qs:
                            qs = qs[0]
                            qs = parse_qs(qs, True)
                        else:
                            qs = None

                        # if debug:
                        #     log.debug(f'[#{handler_id}] query {qs}')

                        body = None

                        if content_length is None:
                            # TODO: no content-length provided, read as stream?
                            pass
                        elif content_length > 0:
                            body = await reader.read(content_length)

                        request = Request(method=method, path=path, query=qs, headers=headers, content_type=content_type, content_length=content_length, body=body,
                                          remote_addr=remote_addr, id=handler_id)

                        response = await self._handle_request(request)

                        response.id = request.id
                        response.remote_addr = request.remote_addr

                        self._write_response(writer, response)

                        if not expect_next_request:
                            break
                        continue

                except Exception as e:
                    tb = traceback.format_exc()
                    log.debug(f'[#{handler_id}] Exception traceback during handling connection from {remote_addr}:\n{tb}')
                    log.error(f'[#{handler_id}] Exception during handling connection from {remote_addr}: {e.__class__.__name__}: {e}')
                    self._write_response(writer, remote_addr, '500 Internal Server Error', body=tb if debug else 'Internal error', handler_id=handler_id)

        except asyncio.TimeoutError:
            log.info(f'[#{handler_id}] Connection {remote_addr} timed out')
        finally:
            log.info(f'[#{handler_id}] Closed connection from {remote_addr}')
            writer.close()

    async def run(self, host='localhost', port=8080, timeout=30, ssl=None):

        if host == '' or host == '*':
            host = '0.0.0.0'

        async def _handler(reader, writer):
            asyncio.create_task(self._session(reader, writer, timeout))

        server = await asyncio.start_server(
            _handler, host=host, port=port, ssl=ssl
        )

        addrs = ', '.join(':'.join(map(str, sock.getsockname()[:2])) for sock in server.sockets)

        self.log.info(f'Serving on {addrs}')

        async with server:
            await server.serve_forever()


ext_to_mime_type = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.htm': 'text/html',
        '.html': 'text/html',
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.json': 'application/json',
}

def serve_file(path, basedir=os.path.normpath(os.path.dirname(__file__)), log=None):
    if log:
        log.debug(f'Serving file {path} at basedir={basedir}')
    if not path or path.endswith('/'):
        path += 'index.html'
    path = os.path.normpath(path)
    if path.startswith('../'):
        # we cannot allow paths above the basedir
        return
    if path.startswith('/'):
        path = path[1:] # strip the /
    path = os.path.join(basedir, path)
    if log:
        log.debug(f'Resolved full path {path}')
    try:
        _, ext = os.path.splitext(path)
        # if log:
        #     log.debug(f'extension {path}')
        content_type = 'application/octet-stream'
        content_type = ext_to_mime_type.get(ext, content_type)
        # if log:
        #     log.debug(f'content type {content_type}')
        with open(path, 'rb') as f:
            data = f.read()
            return dict(headers={'Content-Type': content_type}, status='200 OK', body=data)
    except:
        return dict(headers={}, status='404 Not found')




if __name__ == '__main__':

    import argparse, sys

    parser = argparse.ArgumentParser()
    parser.add_argument('--host', '-a', metavar='HOST', type=str, default='*', help='listening host')
    parser.add_argument('--port', '-p', metavar='PORT', type=int, default=8080, help='listening port')
    parser.add_argument('--ssl', '-s', action='store_true', help='enable SSL')
    parser.add_argument('--cert', '-c', metavar='CERT', type=str, default='', help='path to SSL certificate')
    parser.add_argument('--key', '-k', metavar='KEY', type=str, default='', help='path to SSL key')
    parser.add_argument('--debug', '-d', action='store_true', help='debug mode')
    parser.add_argument('--cors', action='store_true', help='add CORS headers (allow all)')
    parser.add_argument('path', type=str, default='.', help='serve files from path')

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    server = AsyncServer(debug=args.debug)

    basedir = args.path if os.path.isabs(args.path) else os.path.abspath(args.path)

    @server.middleware()
    async def print_headers_middleware(request, call_next):
        print('Request headers:', request.headers)
        response = await call_next(request)
        print('Response headers:', response.headers)
        return response

    if args.cors:
        @server.middleware()
        async def cors_middleware(request, call_next):
            response = await call_next(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods'] = '*'
            response.headers['Access-Control-Allow-Headers'] = '*'
            return response

    @server.handle('GET', ['/{path:path}'])
    async def index_handler(request, path=None):
        return serve_file(path, basedir=basedir)

    ssl_context = None

    if args.ssl:
        if not args.cert or not os.path.isfile(args.cert):
            print('certificate not provided', file=sys.stderr)
            sys.exit(1)
        if not args.key or not os.path.isfile(args.key):
            print('certificate key not provided', file=sys.stderr)
            sys.exit(1)

        import ssl

        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(certfile=args.cert, keyfile=args.key)

    asyncio.run(server.run(host=args.host, port=args.port, ssl=ssl_context))
