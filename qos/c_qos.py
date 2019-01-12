# c_qos.py Client-side application demo for Quality of Service

# Released under the MIT licence.
# Copyright (C) Peter Hinch 2018

import gc
import uasyncio as asyncio

gc.collect()
import ujson
from machine import Pin
from . import local

gc.collect()
from micropython_iot import client


class App:
    def __init__(self, loop, my_id, server, port, timeout, verbose):
        self.verbose = verbose
        self.timeout = timeout
        led = Pin(2, Pin.OUT, value=1)  # Optional LED
        self.cl = client.Client(loop, my_id, server, port, timeout, None, None, verbose, led)
        self.tx_msg_id = 0
        self.dupes = 0  # Incoming dupe count
        self.missing = 0
        self.last = 0
        self.rxbuf = []
        loop.create_task(self.start(loop))

    async def start(self, loop):
        self.verbose and print('App awaiting connection.')
        await self.cl
        loop.create_task(self.reader())
        loop.create_task(self.writer())

    async def reader(self):
        self.verbose and print('Started reader')
        while True:
            header, line = await self.cl.readline()
            data = ujson.loads(line)
            rxmid = data[0]
            if rxmid in self.rxbuf:
                self.dupes += 1
            else:
                self.rxbuf.append(rxmid)
            print('Got', data, 'from server app')

    def count_missed(self):
        if len(self.rxbuf) >= 25:
            idx = 0
            while self.rxbuf[idx] < self.last + 10:
                idx += 1
            self.last += 10
            self.missing += 10 - idx
            self.rxbuf = self.rxbuf[idx:]
        return self.missing

    # Send [ID, (re)connect count, free RAM, duplicate message count, missed msgcount]
    async def writer(self):
        self.verbose and print('Started writer')
        while True:
            gc.collect()
            data = [self.tx_msg_id, self.cl.connects, gc.mem_free(),
                    self.dupes, self.count_missed()]
            self.tx_msg_id += 1
            print('Sent', data, 'to server app\n')
            dstr = ujson.dumps(data)
            await self.cl.write(dstr)
            await asyncio.sleep(5)

    def close(self):
        self.cl.close()


loop = asyncio.get_event_loop()
app = App(loop, local.MY_ID, local.SERVER, local.PORT, local.TIMEOUT, True)
try:
    loop.run_forever()
finally:
    app.close()
