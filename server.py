# server_cp.py Server for IOT communications.

# Released under the MIT licence.
# Copyright (C) Peter Hinch 2018

# Maintains bidirectional full-duplex links between server applications and
# multiple WiFi connected clients. Each application instance connects to its
# designated client. Connections are resilient and recover from outages of WiFi
# and of the connected endpoint.
# This server and the server applications are assumed to reside on a device
# with a wired interface on the local network.

# Run under CPython 3.5+ or MicroPython Unix build
import sys

upython = sys.implementation.name == 'micropython'
if upython:
    import usocket as socket
    import uasyncio as asyncio
    import utime as time
    import uselect as select
    import uerrno as errno
    from . import primitives

    Lock = primitives.Lock
else:
    import socket
    import asyncio
    import time
    import select
    import errno

    Lock = asyncio.Lock


# Read the node ID. There isn't yet a Connection instance.
# CPython does not have socket.readline. Return client_id.

# Note re OSError: did detect errno.EWOULDBLOCK. Not supported in MicroPython.
# In cpython EWOULDBLOCK == EAGAIN == 11.
async def _read(loop, conns, verbose, sock):
    while True:
        # Start (or restart after outage). Do this promptly.
        # Fast version of await self._status_coro()
        buf = bytearray()
        start = time.time()
        client_id = None
        client = None
        while client_id is None or client.status():
            try:
                d = sock.recv(4096)
            except OSError as e:
                err = e.args[0]
                if err == errno.EAGAIN:  # Would block: try later
                    if time.time() - start > TO_SECS:
                        verbose and print('fail detected')  # Unless it timed out.
                        sock.close()
                        return
                    else:
                        # Waiting for data from client. Limit CPU overhead.
                        await asyncio.sleep(TIM_TINY)
                else:
                    verbose and print('connection reset by peer')  # Reset by peer 104
                    sock.close()
                    return
            else:
                start = time.time()  # Something was received
                if d == b'':  # Reset by peer
                    verbose and print('connection reset by peer')  # Reset by peer 104
                    sock.close()
                    return
                buf.extend(d)
                l = bytes(buf).decode().split('\n')
                if client_id is None:
                    client_id = l.pop(0)
                    if client_id in conns:  # Old client, new socket
                        if conns[client_id].status():
                            print('Duplicate client {} ignored.'.format(client_id))
                            sock.close()
                            return
                        else:  # Reconnect after failure
                            conns[client_id].sock = sock
                            client = conns[client_id]
                            verbose and print("Client {!s} reconnected".format(client_id))
                            # assume that the Connection instance does not get removed before the sock does
                    else:
                        verbose and print('Got connection from client', client_id)
                        client = Connection(loop, client_id, sock, verbose)
                        conns[client_id] = client
                if len(l) > 1:  # Have at least 1 newline
                    client.lines.extend(l[:-1])
                    buf = bytearray(l[-1].encode('utf8'))


# API: application calls server.run()
# Allow 2 extra connections. This is to cater for error conditions like
# duplicate or unexpected clients. Accept the connection and have the
# Connection class produce a meaningful error message.
async def run(loop, expected, verbose=False, port=8123, timeout=1500):
    addr = socket.getaddrinfo('0.0.0.0', port, 0, socket.SOCK_STREAM)[0][-1]
    s_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # server socket
    s_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s_sock.bind(addr)
    s_sock.listen(len(expected) + 2)
    global TO_SECS
    global TIMEOUT
    global TIM_SHORT
    global TIM_TINY
    TIMEOUT = timeout
    TO_SECS = timeout / 1000  # ms to seconds
    TIM_SHORT = TO_SECS / 10  # Delay << timeout
    TIM_TINY = 0.05  # Short delay avoids 100% CPU utilisation in busy-wait loops
    verbose and print('Awaiting connection.', port)
    poller = select.poll()
    poller.register(s_sock, select.POLLIN)
    _conns = Connection.go(s_sock, expected)
    while True:
        res = poller.poll(1)  # 1ms block
        if len(res):  # Only s_sock is polled
            c_sock, _ = s_sock.accept()  # get client socket
            c_sock.setblocking(False)
            loop.create_task(_read(loop, _conns, verbose, c_sock))
        await asyncio.sleep(0.2)


# A Connection persists even if client dies (minimise object creation).
# If client dies Connection is closed: ._close() flags this state by closing its
# socket and setting .sock to None (.status() == False).
class Connection:
    _conns = {}  # index: client_id. value: Connection instance
    _expected = set()  # Expected client_id's
    _server_sock = None

    @classmethod
    def go(cls, s_sock, expected):
        if cls._server_sock is None:  # 1st invocation
            cls._server_sock = s_sock
            cls._expected.update(expected)
        return cls._conns

    # Server-side app waits for a working connection
    @classmethod
    async def client_conn(cls, client_id):
        while True:
            if client_id in cls._conns:
                c = cls._conns[client_id]
                # await c 
                # works but under CPython produces runtime warnings. So do:
                await c._status_coro()
                return c
            await asyncio.sleep(0.5)

    # App waits for all expected clients to connect.
    @classmethod
    async def wait_all(cls, client_id=None, peers=None):
        conn = None
        if client_id is not None:
            conn = await client_conn(client_id)
        if peers is None:  # Wait for all expected clients
            while len(cls._expected):
                await asyncio.sleep(0.5)
        else:
            while not set(cls._conns.keys()).issuperset(peers):
                await asyncio.sleep(0.5)
        return conn

    @classmethod
    def close_all(cls):
        for conn in cls._conns.values():
            conn._close()
        if cls._server_sock is not None:
            cls._server_sock.close()

    def __init__(self, loop, client_id, c_sock, verbose):
        self.sock = c_sock  # Socket
        self.client_id = client_id
        self.verbose = verbose
        try:
            Connection._expected.remove(self.client_id)
        except KeyError:
            print('Unknown client {} has connected. Expected {}.'.format(
                self.client_id, Connection._expected))
        self.lock = Lock()
        loop.create_task(self._keepalive())
        self.lines = []

    def status(self):
        return self.sock is not None

    def __await__(self):
        if upython:
            while not self.status():
                yield TIM_SHORT
        else:  # CPython: Meet requirement for generator in __await__
            return self._status_coro().__await__()

    __iter__ = __await__

    async def _status_coro(self):
        while not self.status():
            await asyncio.sleep(TIM_SHORT)

    async def readline(self):
        while True:
            if self.verbose and not self.status():
                print('Reader Client:', self.client_id, 'awaiting OK status')
            await self._status_coro()
            self.verbose and print('Reader Client:', self.client_id, 'OK')
            while self.status():
                if len(self.lines):
                    line = self.lines.pop(0)
                    if len(line):  # Ignore keepalives
                        return line + '\n'
                await asyncio.sleep(TIM_TINY)  # Limit CPU utilisation
            self.verbose and print('Read client disconnected: closing connection.')
            self._close()

    async def _keepalive(self):
        to = TO_SECS * 2 / 3
        while True:
            await self.write('\n')
            await asyncio.sleep(to)

    async def write(self, buf, pause=True):
        if not buf.startswith('\n'):
            end = time.time() + TO_SECS
            if not buf.endswith('\n'):
                buf = ''.join((buf, '\n'))
        while True:
            if self.verbose and not self.status():
                print('Writer Client:', self.client_id, 'awaiting OK status')
            await self._status_coro()
            self.verbose and print('Writer Client:', self.client_id, 'OK')
            try:
                async with self.lock:  # >1 writing task?
                    await self._send(buf)  # OSError on fail
                break
            except OSError:
                self.verbose and print('Write client disconnected: closing connection.')
                self._close()
        if pause and not buf.startswith('\n'):  # Throttle rate of non-keepalive messages
            # Kevin Köck: does not have any effect if multiple coroutines try to write
            dt = end - time.time()
            if dt > 0:
                await asyncio.sleep(dt)  # Control tx rate: <= 1 msg per timeout period

    async def _send(self, d):
        if not self.status():
            raise OSError
        d = d.encode('utf8')
        start = time.time()
        while len(d):
            try:
                ns = self.sock.send(d)  # Raise OSError if client fails
            except OSError:
                raise
            else:
                d = d[ns:]
                if len(d):
                    await asyncio.sleep(TIM_SHORT)
                    if (time.time() - start) > TO_SECS:
                        raise OSError

    def __getitem__(self, client_id):  # Return a Connection of another client
        return Connection._conns[client_id]

    def _close(self):
        if self.sock is not None:
            self.verbose and print('fail detected')
            if self.sock is not None:
                self.sock.close()
                self.sock = None


# API aliases
client_conn = Connection.client_conn
wait_all = Connection.wait_all
