# cli.py Test of socket. Run on Pyboard D

import gc

gc.collect()
import usocket as socket
import uasyncio as asyncio
import ujson as json
import utime as time
import errno

gc.collect()
import network

PORT = 8888
SERVER = '192.168.178.60'
ACK = -1


class Lock:
    def __init__(self, delay_ms=0):
        self._locked = False
        self.delay_ms = delay_ms

    def locked(self):
        return self._locked

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *args):
        self.release()
        await asyncio.sleep(0)

    async def acquire(self):
        while True:
            if self._locked:
                await asyncio.sleep_ms(self.delay_ms)
            else:
                self._locked = True
                break

    def release(self):
        if not self._locked:
            raise RuntimeError('Attempt to release a lock which has not been set')
        self._locked = False


lock = Lock()


async def run(loop):
    s = network.WLAN()
    print('Waiting for WiFi')  # ESP8266 with stored connection
    while not s.isconnected():
        await asyncio.sleep_ms(200)
    print('WiFi OK')
    sock = socket.socket()
    try:
        serv = socket.getaddrinfo(SERVER, PORT)[0][-1]  # server read
        # If server is down OSError e.args[0] = 111 ECONNREFUSED
        sock.connect(serv)
    except OSError:
        print('Connect fail.')
        return
    sock.setblocking(False)
    loop.create_task(reader(sock))
    loop.create_task(writer(sock))
    loop.create_task(simulate_async_delay())


async def simulate_async_delay():
    while True:
        await asyncio.sleep(0)
        time.sleep(0.05)  # 0.2 eventually get long delays


async def reader(sock):
    print('Reader start')
    ack = '{}\n'.format(json.dumps([ACK, 'Ack from client.']))
    last = -1
    while True:
        line = await readline(sock)
        data = json.loads(line)
        if data[0] != ACK:
            await send(sock, ack.encode('utf8'))
            print('Got', data)
            if last >= 0 and data[0] - last - 1:
                raise OSError('Missed message')
        last = data[0]


async def writer(sock):
    print('Writer start')
    data = [0, 'Message from client.']
    while True:
        for _ in range(4):
            d = '{}\n'.format(json.dumps(data))
            await send(sock, d.encode('utf8'))
            data[0] += 1
        await asyncio.sleep_ms(1030)  # ???


async def readline(sock):
    line = b''
    while True:
        if line.endswith(b'\n'):
            return line.decode()
        async with lock:
            d = sock.readline()
        if d == b'':
            raise OSError
        if d is not None:  # Something received
            line = b''.join((line, d))
        await asyncio.sleep(0)


async def send(sock, d):  # Write a line to socket.
    while d:
        try:
            async with lock:
                ns = sock.send(d)
        except OSError as e:
            err = e.args[0]
            if err == errno.EAGAIN:  # Would block: try later
                await asyncio.sleep_ms(100)
        else:
            d = d[ns:]
            if d:  # Partial write: trim data and pause
                await asyncio.sleep_ms(20)


loop = asyncio.get_event_loop()
loop.create_task(run(loop))
loop.run_forever()
