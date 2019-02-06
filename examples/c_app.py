# c_app.py Client-side application demo

# Released under the MIT licence.
# Copyright (C) Peter Hinch 2018

import gc
import uasyncio as asyncio

gc.collect()
from micropython_iot import client

gc.collect()
import ujson
# Optional LED. led=None if not required
from sys import platform
if platform == 'pyboard':  # D series
    from pyb import LED
    led = LED(1)
else:
    from machine import Pin
    led = Pin(2, Pin.OUT, value=1)  # Optional LED
# End of optionalLED

from . import local

gc.collect()


class App(client.Client):
    def __init__(self, loop, verbose):
        self.verbose = verbose
        self.cl = client.Client(loop, local.MY_ID, local.SERVER, local.SSID, local.PW,
                         conn_cb=self.constate, verbose=verbose, led=led, wdog=False)
        loop.create_task(self.start(loop))
        self.latency_added = 0
        self.count = 0

    async def start(self, loop):
        self.verbose and print('App awaiting connection.')
        await self.cl
        loop.create_task(self.rreader())
        loop.create_task(self.wwriter())

    async def rreader(self):
        import utime
        while True:
            header, line = await self.cl.readline()
            data = ujson.loads(line)
            latency = utime.ticks_ms() - data[0]
            self.latency_added += latency
            self.count += 1
            print("Latency:", latency, "Avg Latency:", self.latency_added / self.count)

    async def wwriter(self):
        import utime
        while True:
            await asyncio.sleep(1)
            data = [utime.ticks_ms()]
            await self.cl.write(None, ujson.dumps(data))

    def constate(self, state):
        print("Connection state:", state)

    async def reader(self):
        self.verbose and print('Started reader')
        while True:
            # Attempt to read data: in the event of an outage, .readline()
            # pauses until the connection is re-established.
            header, line = await self.cl.readline()
            data = ujson.loads(line)
            # Receives [restart count, uptime in secs]
            print('Got', header, data, 'from server app')

    # Send [approx application uptime in secs, (re)connect count]
    async def writer(self):
        self.verbose and print('Started writer')
        data = [0, 0, 0]
        count = 0
        while True:
            data[0] = self.cl.connects
            data[1] = count
            count += 1
            gc.collect()
            data[2] = gc.mem_free()
            print('Sent', data, 'to server app\n')
            # .write() behaves as per .readline()
            await self.cl.write(None, ujson.dumps(data))
            await asyncio.sleep(5)

    def shutdown(self):
        self.cl.close()  # Shuts down WDT (but not on Pyboard D).


loop = asyncio.get_event_loop()
app = App(loop, verbose=True)
try:
    loop.run_forever()
finally:
    app.shutdown()
