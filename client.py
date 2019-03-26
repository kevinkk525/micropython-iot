# client.py Client class for resilient asynchronous IOT communication link.

# Released under the MIT licence.
# Copyright (C) Peter Hinch, Kevin Köck 2019

# After sending ID now pauses before sending further data to allow server to
# initiate read task.

import gc

gc.collect()
import usocket as socket
from ucollections import deque
import uasyncio as asyncio

gc.collect()
from sys import platform
import network
import utime
import machine
import errno
from . import gmid, isnew, launch, Event, Lock  # __init__.py
import ubinascii
import uio
import ujson

gc.collect()
from micropython import const

WDT_CANCEL = const(-2)
WDT_CB = const(-3)

# Message ID generator. Only need one instance on client.
getmid = gmid()
gc.collect()


class Client:
    def __init__(self, loop, my_id, server, port=9999,
                 ssid='', pw='', timeout=4000,
                 conn_cb=None, conn_cb_args=None,
                 verbose=False, led=None, wdog=False):
        """
        Create a client connection object
        :param loop: uasyncio loop
        :param my_id: client id
        :param server: server address/ip
        :param port: port the server app is running on
        :param timeout: connection timeout
        :param conn_cb: cb called when (dis-)connected to server
        :param conn_cb_args: optional args to pass to connected_cb
        :param verbose: debug output
        :param led: led output for showing connection state, heartbeat
        :param wdog: use a watchdog to prevent the device from getting stuck/frozen/...
        writes to ensure that all messages are sent in order even if an outage occurs.
        """
        self._loop = loop
        self._my_id = my_id
        self._server = server
        self._ssid = ssid
        self._pw = pw
        self._port = port
        self._to = timeout  # Client and server timeout
        self._tim_short = timeout // 10
        self._tim_ka = timeout // 4  # Keepalive interval
        self._concb = conn_cb
        self._concbargs = () if conn_cb_args is None else conn_cb_args
        self._verbose = verbose
        self._led = led

        if wdog:
            if platform == 'pyboard':
                self._wdt = machine.WDT(0, 20000)

                def wdt():
                    def inner(feed=0):  # Ignore control values
                        if not feed:
                            self._wdt.feed()

                    return inner

                self._feed = wdt()
            else:
                def wdt(secs=0):
                    timer = machine.Timer(-1)
                    timer.init(period=1000, mode=machine.Timer.PERIODIC,
                               callback=lambda t: self._feed())
                    cnt = secs
                    run = False  # Disable until 1st feed

                    def inner(feed=WDT_CB):
                        nonlocal cnt, run, timer
                        if feed == 0:  # Fixed timeout
                            cnt = secs
                            run = True
                        elif feed < 0:  # WDT control/callback
                            if feed == WDT_CANCEL:
                                timer.deinit()  # Permanent cancellation
                            elif feed == WDT_CB and run:  # Timer callback and is running.
                                cnt -= 1
                                if cnt <= 0:
                                    machine.reset()

                    return inner

                self._feed = wdt(20)
        else:
            self._feed = lambda x: None

        self._sta_if = network.WLAN(network.STA_IF)
        ap = network.WLAN(network.AP_IF)  # create access-point interface
        ap.active(False)  # deactivate the interface

        self._sta_if.active(True)
        gc.collect()

        self._evfail = Event(100)  # 100ms pause
        self._s_lock = Lock()  # For internal send conflict.
        self._last_wr = utime.ticks_ms()
        self._lineq = deque((), 5, True)  # 5 entries, throw on overflow
        self.connects = 0  # Connect count for test purposes/app access
        self._sock = None
        self._ok = False  # Set after 1st successful read
        self._tx_mid = 0  # sent mid +1, used for keeping messages in order
        self._ack_pend = -1  # ACK which is expected to be received

        gc.collect()
        loop.create_task(self._run(loop))

    # **** API ****
    def __iter__(self):  # Await a connection
        while not self():
            yield from asyncio.sleep_ms(self._tim_short)

    __await__ = __iter__  # not needed but suppresses warnings in IDE

    def status(self) -> bool:
        """
        Returns the state of the connection
        :return: bool
        """
        return self._ok

    __call__ = status

    async def readline(self) -> any:
        """
        Reads one line
        :return: string
        """
        h, d = await self.read()
        return d

    async def read(self) -> (bytearray, any):
        """
        Reads one message containing header and line.
        Header can be None if not used.
        :return: header, line
        """
        while not self._lineq:
            await asyncio.sleep(0)
        return self._lineq.popleft()

    async def writeline(self, buf: any, qos: bool = True):
        """
        Write one line.
        :param buf: str/bytes
        :param qos: bool
        :return: None
        """
        await self.write(None, buf, qos)

    async def write(self, header: bytearray, buf: any, qos: bool = True):
        """
        Send a new message containing header and line
        :param header: optional user header, pass None if not used
        :param buf: string/byte/None, message to be sent
        :param qos: bool
        :return: None
        """
        if buf is None:
            buf = b''
        mid = next(getmid)
        preheader = bytearray(3)
        preheader[0] = mid
        preheader[1] = 0 if header is None else len(header)
        preheader[2] = 0  # special internal usages, e.g. for esp_link or ACKs
        if qos:
            preheader[2] |= 0x01  # qos==True, request ACK
        preheader = ubinascii.hexlify(preheader)
        if header is not None:
            if type(header) != bytearray:
                raise TypeError("Header has to be bytearray")
            else:
                header = ubinascii.hexlify(header)
        buf = ujson.dumps(buf)  # conversion done here to ensure that the content
        # won't change although it could increase RAM usage if many write() are started
        while self._tx_mid != mid or self._ok is False:
            # keeps order even with multiple writes and waits for connection to be ok
            await asyncio.sleep_ms(50)
        await self._write(preheader, header, buf, qos, mid)

    def close(self):
        """
        Close the connection. Closes the socket.
        :return:
        """
        self._close()  # Close socket and WDT
        self._feed(WDT_CANCEL)

    # **** For subclassing ****

    # May be overridden e.g. to provide timeout (asyncio.wait_for)
    async def bad_wifi(self):
        """
        Called if the initial wifi connection is not possible.
        Subclass to implement a solution.
        :return:
        """
        if not self._ssid:
            raise OSError('No initial WiFi connection.')
        s = self._sta_if
        if s.isconnected():
            return
        while True:  # For the duration of an outage
            s.connect(self._ssid, self._pw)
            if await self._got_wifi(s):
                break

    async def bad_server(self):
        """
        Called if the initial connection to the server was not possible.
        Subclass to implement a solution.
        :return:
        """
        await asyncio.sleep(0)
        raise OSError('No initial server connection.')

    # **** API end ****

    def _close(self):
        self._verbose and print('Closing sockets.')
        if self._sock is not None:  # ESP32 issue #4514
            self._sock.close()

    # Await a WiFi connection for 10 secs.
    async def _got_wifi(self, s):
        for _ in range(20):  # Wait t s for connect. If it fails assume an outage
            await asyncio.sleep_ms(500)
            self._feed(0)
            if s.isconnected():
                return True
        return False

    async def _write(self, preheader, header, buf, qos, mid, ack=False):
        if buf is None:
            buf = b''
        if not self._ok and ack is True:
            return  # server will resend message
        while True:
            # After an outage wait until something is received from server
            # before we send.
            await self
            try:
                async with self._s_lock:
                    await self._send(preheader)
                    if header is not None:
                        await self._send(header)
                    await self._send(buf)
                    if buf.endswith(b"\n") is False:
                        await self._send(b"\n")
                    if platform == 'pyboard':
                        await asyncio.sleep_ms(200)  # Reduce message loss (why ???)
                        # TODO: Still needed? I don't have a pyboard D
                self._verbose and print('Sent data', preheader, header, buf, qos)
            except OSError:
                self._evfail.set('writer fail')
                # Wait for a response to _evfail
                while self._ok:
                    await asyncio.sleep_ms(self._tim_short)
                continue
            if qos is False:
                return True
            else:
                st = utime.ticks_ms()
                while mid != self._ack_pend and utime.ticks_diff(utime.ticks_ms(), st) < (self._to / 2):
                    await asyncio.sleep_ms(20)
                if mid != self._ack_pend:  # waited for ACK for 1/2 timeout period
                    self._verbose and print(utime.ticks_ms(), "ack not received")
                    continue
                self._tx_mid += 1
                if self._tx_mid == 256:
                    self._tx_mid = 1
                return True

    # Make an attempt to connect to WiFi. May not succeed.
    async def _connect(self, s):
        self._verbose and print('Connecting to WiFi')
        if platform == 'esp8266':
            s.connect()
        elif self._ssid:
            s.connect(self._ssid, self._pw)
        else:
            raise ValueError('No WiFi credentials available.')

        # Break out on success (or fail after 10s).
        await self._got_wifi(s)
        self._verbose and print('Checking WiFi stability for 3s')
        # Timeout ensures stable WiFi and forces minimum outage duration
        await asyncio.sleep(3)
        self._feed(0)

    async def _run(self, loop):
        # ESP8266 stores last good connection. Initially give it time to re-establish
        # that link. On fail, .bad_wifi() allows for user recovery.
        await asyncio.sleep(1)  # Didn't always start after power up
        s = self._sta_if
        if platform == 'esp8266':
            s.connect()
            for _ in range(4):
                await asyncio.sleep(1)
                if s.isconnected():
                    break
            else:
                await self.bad_wifi()
        else:
            await self.bad_wifi()
        init = True
        while True:
            while not s.isconnected():  # Try until stable for 2*.timeout
                await self._connect(s)
            self._verbose and print('WiFi OK')
            self._sock = socket.socket()
            self._evfail.clear()
            _reader = self._reader()
            try:
                serv = socket.getaddrinfo(self._server, self._port)[0][-1]  # server read
                # If server is down OSError e.args[0] = 111 ECONNREFUSED
                self._sock.connect(serv)
            except OSError as e:
                if e.args[0] in (errno.ECONNABORTED, errno.ECONNRESET, errno.ECONNREFUSED):
                    if init:
                        await self.bad_server()
            else:
                self._sock.setblocking(False)
                # Start reading before server can send: can't send until it
                # gets ID.
                loop.create_task(_reader)
                # Server reads ID immediately, but a brief pause is probably wise.
                await asyncio.sleep_ms(50)
                preheader = bytearray(3)
                preheader[0] = 0x2C  # mid but in this case protocol identifier but will receive an ACK with mid 0x2C
                preheader[1] = 0  # user/app header length
                preheader[2] = 0xFF  # clean connection, shows if device has been reset or just a wifi outage
                preheader = ubinascii.hexlify(preheader)
                try:
                    # not sending as qos message. Using server keepalive as ACK
                    await self._send(preheader)
                    await self._send(self._my_id)
                    await self._send(b"\n")
                except OSError:
                    self._verbose and print("Sending id failed")
                    if init:  # another application could run on this port and reject messages
                        await self.bad_server()
                else:
                    _keepalive = self._keepalive()
                    loop.create_task(_keepalive)
                    if self._concb is not None:
                        # apps might need to know connection to the server acquired
                        launch(self._concb, True, *self._concbargs)
                    await self._evfail  # Pause until something goes wrong
                    self._verbose and print(self._evfail.value())
                    self._ok = False
                    asyncio.cancel(_reader)
                    asyncio.cancel(_keepalive)
                    await asyncio.sleep_ms(0)  # wait for cancellation
                    self._feed(0)  # _concb might block (I hope not)
                    if self._concb is not None:
                        # apps might need to know if they lost connection to the server
                        launch(self._concb, False, *self._concbargs)
            finally:
                init = False
                self._close()  # Close socket but not wdt
                s.disconnect()
                self._feed(0)
                # await asyncio.sleep_ms(self._to * 2)  # Ensure server detects outage.
                # Not needed as the expected client_id is a device specific hardware id
                # therefore a misconfigured client can't hijack an existing connection.
                while s.isconnected():
                    await asyncio.sleep(1)

    async def _reader(self):  # Entry point is after a (re) connect.
        c = self.connects  # Count successful connects
        while True:
            try:
                preheader, header, line = await self._readline()  # OSError on fail
            except OSError:
                self._evfail.set('reader fail')  # ._run cancels other coros
                return
            mid = preheader[0]
            if preheader[2] & 0x2C == 0x2C:  # ACK
                self._verbose and print("Got ack mid", mid)
                self._ack_pend = mid
                continue  # All done
            # Discard dupes. mid == 0 : Server has power cycled
            if not mid:
                isnew(-1)  # Clear down rx message record
            if isnew(mid):
                try:
                    self._lineq.append((header, line))
                except IndexError:
                    self._evfail.set('_reader fail. Overflow.')
                    return
            if preheader[2] & 0x01 == 1:  # qos==True, send ACK even if dupe
                await self._sendack(mid)
            if c == self.connects:
                self.connects += 1  # update connect count

    async def _sendack(self, mid):
        if self._ok is False:
            return
        preheader = bytearray(3)
        preheader[0] = mid
        preheader[1] = 0
        preheader[2] = 0x2C  # ACK
        await self._write(ubinascii.hexlify(preheader), None, None, qos=False, mid=mid, ack=True)
        # ACK does not get qos as server will resend message if outage occurs

    async def _keepalive(self):
        while True:
            due = self._tim_ka - utime.ticks_diff(utime.ticks_ms(), self._last_wr)
            if due <= 0:
                async with self._s_lock:
                    # error sets ._evfail, .run cancels this coro
                    await self._send(b'\n')
            else:
                await asyncio.sleep_ms(due)

    # Read a line from nonblocking socket: reads can return partial data which
    # are joined into a line. Blank lines are keepalive packets which reset
    # the timeout: _readline() pauses until a complete line has been received.
    async def _readline(self) -> (bytearray, bytearray, any):
        led = self._led
        line = b''
        start = utime.ticks_ms()
        while True:
            if line.endswith(b'\n'):
                self._ok = True  # Got at least 1 packet after an outage.
                if len(line) > 1:
                    d = uio.StringIO(line)
                    try:
                        preheader = bytearray(ubinascii.unhexlify(d.read(6)))
                        if preheader[1] != 0:
                            header = bytearray(ubinascii.unhexlify(d.read(preheader[1] * 2)))
                        else:
                            header = None
                        if len(line) > 7 + preheader[1] * 2:  # preheader+header+\n
                            line = ujson.load(d)
                        else:
                            line = b""
                    except ValueError as e:
                        print("Error converting message:", d.read(), "error:", e)
                        line = b''
                        continue
                    except Exception as e:
                        print("Error converting sth", e)
                        continue
                    finally:
                        d.close()
                        gc.collect()
                    return preheader, header, line
                # Got a keepalive: discard, reset timers, toggle LED.
                self._feed(0)
                line = b''
                if led is not None:
                    if isinstance(led, machine.Pin):
                        led(not led())
                    else:  # On Pyboard D
                        led.toggle()
            try:
                d = self._sock.readline()
            except Exception as e:
                self._verbose and print('_readline exception')
                raise
            if d == b'':
                self._verbose and print('_readline peer disconnect')
                raise OSError
            if d is None:  # Nothing received: wait on server
                if utime.ticks_diff(utime.ticks_ms(), start) > self._to:
                    self._verbose and print('_readline timeout')
                    raise OSError
                await asyncio.sleep_ms(0)
            else:  # Something received: reset timer
                start = utime.ticks_ms()
                line = b''.join((line, d)) if line else d

    async def _send(self, d):  # Write a string/byte/bytearray to socket.
        start = utime.ticks_ms()
        while d:
            try:
                ns = self._sock.send(d)  # OSError if client closes socket
            except OSError as e:
                err = e.args[0]
                if err == errno.EAGAIN:  # Would block: await server read
                    await asyncio.sleep_ms(100)
                else:
                    self._evfail.set('_send fail. Disconnect')
                    return False  # peer disconnect
            else:
                d = d[ns:]
                if d:  # Partial write: pause
                    await asyncio.sleep_ms(20)
                if utime.ticks_diff(utime.ticks_ms(), start) > self._to:
                    self._evfail.set('_send fail. Timeout.')
                    return False

        self._last_wr = utime.ticks_ms()
        return True
