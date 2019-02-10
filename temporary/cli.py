# cli.py Test of socket. Run on Pyboard D

import gc

gc.collect()
import usocket as socket
import uasyncio as asyncio
import ujson as json
import errno

gc.collect()
import network
import ubinascii
import machine

MY_ID = ubinascii.hexlify(machine.unique_id()).decode()
PORT = 8888
SERVER = '192.168.178.60'


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


async def reader(sock):
    print('Reader start')
    last = -1
    while True:
        line = await readline(sock)
        data = json.loads(line)
        print('Got', data)
        if last >= 0 and data[0] - last - 1:
            raise OSError('Missed message')
        last = data[0]


async def writer(sock):
    print('Writer start')
    data = [0, 'Message from client {!s}.'.format(MY_ID)]
    while True:
        d = '{}\n'.format(json.dumps(data))
        await send(sock, d.encode('utf8'))
        data[0] += 1
        await asyncio.sleep_ms(253)  # ???


async def readline(sock):
    line = b''
    while True:
        if line.endswith(b'\n'):
            return line.decode()
        d = sock.readline()
        if d == b'':
            print("Connection closed")
            raise OSError
        if d is not None:  # Something received
            line = b''.join((line, d))
        await asyncio.sleep(0)


async def send(sock, d):  # Write a line to socket.
    print("Sending", d)
    while d:
        try:
            ns = sock.send(d)
        except OSError as e:
            err = e.args[0]
            if err == errno.EAGAIN:  # Would block: try later
                print("EAGAIN send")
                await asyncio.sleep_ms(100)
        else:
            d = d[ns:]
            if d:  # Partial write: trim data and pause
                await asyncio.sleep_ms(20)


loop = asyncio.get_event_loop()
loop.create_task(run(loop))
loop.run_forever()
