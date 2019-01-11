# client.py Client class for resilient asynchronous IOT communication link.

# Released under the MIT licence.
# Copyright (C) Peter Hinch, Kevin Köck 2018

# After sending ID now pauses before sending further data to allow server to
# initiate read task.

import gc

gc.collect()
import usocket as socket
import uasyncio as asyncio

gc.collect()

import network
import errno
import utime
import ubinascii

gc.collect()
from . import gmid, isnew, launch, Event, Lock  # __init__.py

getmid = gmid()  # Message ID generator
gc.collect()


class Client:
    def __init__(self, loop, my_id, server, port, timeout,
                 connected_cb=None, connected_cb_args=None,
                 verbose=False, led=None):
        self.loop = loop
        self.timeout = timeout  # Server timeout
        self.verbose = verbose
        self.led = led
        self.my_id = my_id  # does not need to be newline-terminated
        self._sta_if = network.WLAN(network.STA_IF)
        self._sta_if.active(True)
        ap = network.WLAN(network.AP_IF)
        ap.active(False)
        self.server = socket.getaddrinfo(server, port)[0][-1]  # server read
        gc.collect()
        self.evfail = Event(100)  # 100ms pause
        self.evread = Event(100)
        self.evsend = Event(100)
        self.wrlock = Lock(100)
        self.lock = Lock(100)
        self.connects = 0  # Connect count for test purposes/app access
        self._concb = connected_cb
        self._concbargs = () if connected_cb_args is None else connected_cb_args
        self.sock = None
        self.ok = False  # Set after 1st successful read
        self._init = True
        gc.collect()
        loop.create_task(self._run(loop))

    # **** API ****
    def __iter__(self):  # App can await a connection
        while not self.ok:
            yield from asyncio.sleep_ms(500)

    __await__ = __iter__

    def status(self):
        return self.ok

    async def readline(self):
        await self.evread
        h, d = self.evread.value()
        self.evread.clear()
        return h, d

    async def write(self, header, buf, pause=True, qos=True, mrt=5):
        """
        Send a new message
        :param header: optional user header, make sure it does not get modified
        :param buf: string/byte, message to be sent
        after sending as it is passed by reference
        :param pause: bool, pause for tx rate limiting
        :param qos: bool
        :param mrt: int, max retries for qos so message does not get retried infinitely on very bad wifi
        :return:
        """
        if len(buf) > 65535:
            raise ValueError("Message longer than 65535")
        preheader = bytearray(5)
        preheader[0] = next(getmid)
        preheader[1] = 0 if header is None else len(header)
        preheader[2] = len(buf) & 0xFF
        preheader[3] = (len(buf) >> 8) & 0xFF  # allows for 65535 message length
        preheader[4] = 0  # special internal usages, e.g. for esp_link
        preheader = ubinascii.hexlify(preheader)
        if header is not None:
            if type(header) != bytearray:
                raise TypeError("Header has to be bytearray")
            else:
                header = ubinascii.hexlify(header)

        async with self.wrlock:  # May be >1 user coro launching .write
            while self.evsend.is_set():  # _writer still busy
                await asyncio.sleep_ms(30)
            end = utime.ticks_add(self.timeout, utime.ticks_ms())
            self.evsend.set((preheader, header, buf))  # Cleared after apparently successful tx
            while self.evsend.is_set():
                await asyncio.sleep_ms(30)
        if qos:  # Retransmit if link has gone down
            self.loop.create_task(self._repeat(preheader, header, buf, mrt))
        if pause:
            dt = utime.ticks_diff(end, utime.ticks_ms())
            if dt > 0:
                await asyncio.sleep_ms(dt)  # Control tx rate: <= 1 msg per timeout period

    def close(self):
        self.verbose and print('Closing sockets.')
        if isinstance(self.sock, socket.socket):
            self.sock.close()

    # **** For subclassing ****

    async def bad_wifi(self):
        await asyncio.sleep(0)
        raise OSError('No initial WiFi connection.')

    async def bad_server(self):
        await asyncio.sleep(0)
        raise OSError('No initial server connection.')

    # **** API end ****

    # qos>0 Repeat tx if outage occurred after initial tx (1st may have been lost)
    async def _repeat(self, preheader, header, buf, mrt=5):
        c = 0
        while True:
            await asyncio.sleep_ms(self.timeout)
            if self.ok:
                return

            async with self.wrlock:
                while self.evsend.is_set():  # _writer still busy
                    await asyncio.sleep_ms(30)
                self.evsend.set((preheader, header, buf))  # Cleared after apparently successful tx
                while self.evsend.is_set():
                    await asyncio.sleep_ms(30)
            self.verbose and print('Repeat', (preheader, header, buf), 'to server app')
            c += 1
            if c >= mrt:
                self.verbose and print("Dumping", (preheader, header, buf), "because max_retry")
                return

    # Make an attempt to connect to WiFi. May not succeed.
    async def _connect(self, s):
        self.verbose and print('Connecting to WiFi')
        s.connect()
        # Break out on fail or success.
        while s.status() == network.STAT_CONNECTING:
            await asyncio.sleep(1)
        t = utime.ticks_ms()
        self.verbose and print('Checking WiFi stability for {}ms'.format(2 * self.timeout))
        # Timeout ensures stable WiFi and forces minimum outage duration
        while s.isconnected() and utime.ticks_diff(utime.ticks_ms(), t) < 2 * self.timeout:
            await asyncio.sleep(1)

    async def _run(self, loop):
        # ESP8266 stores last good connection. Initially give it time to re-establish
        # that link. On fail, .bad_wifi() allows for user recovery.
        s = self._sta_if
        s.connect()
        for _ in range(4):
            await asyncio.sleep(1)
            if s.isconnected():
                break
        else:
            await self.bad_wifi()
        init = True
        while True:
            while not s.isconnected():  # Try until stable for 2*.timeout
                await self._connect(s)
            self.verbose and print('WiFi OK')
            self.sock = socket.socket()
            self.evfail.clear()
            _reader = self._reader()
            try:
                # If server is down OSError e.args[0] = 111 ECONNREFUSED
                self.sock.connect(self.server)
                self.sock.setblocking(False)
                # Start reading before server can send: can't send until it
                # gets ID.
                loop.create_task(_reader)
                # Server reads ID immediately, but a brief pause is probably wise.
                await asyncio.sleep_ms(50)
                preheader = bytearray(5)
                preheader[0] = 0x2C  # mid but in this case protocol identifier
                preheader[1] = 0  # header length
                preheader[2] = len(self.my_id) & 0xFF
                preheader[3] = (len(self.my_id) >> 8) & 0xFF  # allows for 65535 message length
                preheader[4] = init  # clean connection, shows if device has been reset or just a wifi outage
                preheader = ubinascii.hexlify(preheader)
                await self._send(preheader)
                # no header, just preheader
                await self._send(self.my_id)  # Can throw OSError
                await self._send(b"\n")
            except OSError:
                if init:
                    await self.bad_server()
            else:
                # Note _writer pauses before 1st tx
                _writer = self._writer()
                loop.create_task(_writer)
                _keepalive = self._keepalive()
                loop.create_task(_keepalive)
                if self._concb is not None:
                    # apps might need to know connection to the server acquired
                    launch(self._concb, True, *self._concbargs)
                await self.evfail  # Pause until something goes wrong
                self.verbose and print(self.evfail.value())
                self.ok = False
                asyncio.cancel(_reader)
                asyncio.cancel(_writer)
                asyncio.cancel(_keepalive)
                # await asyncio.sleep(1)  # wait for cancellation
                if self._concb is not None:
                    # apps might need to know if they lost connection to the server
                    launch(self._concb, False, *self._concbargs)
                await asyncio.sleep(1)  # wait for cancellation
            finally:
                init = False
                self.close()  # Close socket
                s.disconnect()
                await asyncio.sleep_ms(self.timeout * 2)  # TEST ensure server qos detects
                while s.isconnected():
                    await asyncio.sleep(1)

    async def _reader(self):  # Entry point is after a (re) connect.
        c = self.connects  # Count successful connects
        self.evread.clear()  # No data read yet
        try:
            while True:
                preheader, header, line = await self._readline()  # OSError on fail
                # Discard dupes
                mid = preheader[0]
                # mid == 0 : Server has power cycled
                if not mid:
                    isnew(-1)  # Clear down rx message record
                # _init : client has restarted. mid == 0 server power up
                if self._init or not mid or isnew(mid):
                    self._init = False
                    # Read succeeded: flag .readline
                    if self.evread.is_set():
                        self.verbose and print("Dumping unread message", self.evread.value())
                    self.evread.set((header, line))
                if c == self.connects:
                    self.connects += 1  # update connect count
        except OSError:
            self.evfail.set('reader fail')  # ._run cancels other coros

    async def _writer(self):
        # Need a delay to let server initiate: it can take 0.1*timeout before
        # good status is detected so ensure rx is ready
        await asyncio.sleep_ms(self.timeout // 3)
        # Preclude any chance of rx timeout. Lock not needed yet,
        await self._send(b'\n')
        try:
            while True:
                await self.evsend
                async with self.lock:
                    preheader, header, line = self.evsend.value()
                    self.verbose and print("_write", preheader, header, line)
                    await self._send(preheader)
                    if header is not None:
                        await self._send(header)
                    await self._send(line)
                    if line.endswith("\n") is False:
                        await self._send(b"\n")
                self.verbose and print('Sent data', self.evsend.value())
                self.evsend.clear()  # Sent unless other end has failed and not yet detected
        except OSError:
            self.evfail.set('writer fail')

    async def _keepalive(self):
        tim = self.timeout * 2 // 3  # Ensure  >= 1 keepalives in server t/o
        try:
            while True:
                await asyncio.sleep_ms(tim)
                async with self.lock:
                    await self._send(b'\n')
        except OSError:
            self.evfail.set('keepalive fail')

    # Read a line from nonblocking socket: reads can return partial data which
    # are joined into a line. Blank lines are keepalive packets which reset
    # the timeout: _readline() pauses until a complete line has been received.
    async def _readline(self):
        line = None
        preheader = None
        header = None
        start = utime.ticks_ms()
        while True:
            if preheader is None:
                cnt = 10  # 5
            elif header is None and preheader[1] != 0:
                cnt = preheader[1] * 2
            elif line is None:
                cnt = (preheader[3] << 8) | preheader[2]
                if cnt == 0:
                    line = b""
            else:
                cnt = 1  # only newline-termination missing
            d = await self._read_small(cnt, start)
            # d is not None and print("read small got", d, cnt)
            if d is None:
                self.ok = True  # Got at least 1 complete message or keepalive
                if line is not None:
                    return preheader, header, line.decode()
                line = None
                preheader = None
                header = None
                start = utime.ticks_ms()  # Blank line is keepalive
                if self.led is not None:
                    self.led(not self.led())
                continue
            if preheader is None:
                preheader = bytearray(ubinascii.unhexlify(d))
            elif header is None and preheader[1] != 0:
                header = bytearray(ubinascii.unhexlify(d))
            elif line is None:
                line = d
            else:
                raise OSError  # got unexpected characters instead of \n

    async def _read_small(self, cnt, start):
        m = b''
        rcnt = cnt
        while True:
            try:
                d = self.sock.recv(rcnt)
            except OSError as e:
                if e.args[0] == errno.EAGAIN:
                    await asyncio.sleep_ms(50)
                    continue
                else:
                    raise OSError
            if d == b'':
                raise OSError
            if d is None:  # Nothing received: wait on server
                await asyncio.sleep_ms(100)
            elif d == b"\n":
                return None  # either EOF or keepalive
            elif d.startswith(b"\n"):
                d = d[1:]
            else:
                m = b''.join((m, d))
            if len(m) == cnt:
                return m
            else:
                rcnt = cnt - len(m)
            if utime.ticks_diff(utime.ticks_ms(), start) > self.timeout:
                raise OSError

    async def _send(self, d):  # Write a line to socket.
        start = utime.ticks_ms()
        nts = len(d)  # Bytes to send
        ns = 0  # No. sent
        while ns < nts:
            n = self.sock.send(d)  # OSError if client closes socket
            ns += n
            if ns < nts:  # Partial write: trim data and pause
                d = d[n:]
                await asyncio.sleep_ms(20)
            if utime.ticks_diff(utime.ticks_ms(), start) > self.timeout:
                raise OSError
