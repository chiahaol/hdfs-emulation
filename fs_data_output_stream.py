import asyncio

from collections import deque
from data_streamer import DataStreamer

class FSDataOutputStream:
    @classmethod
    async def create(cls):
        self = FSDataOutputStream()
        self.streamer = await DataStreamer.create()
        self.task = None
        return self

    def __init__(self):
        self.data_queue = asyncio.Queue()

    def open(self):
        self.task = asyncio.create_task(self.streamer.write(self.data_queue))

    async def write(self, data, offset, length):
        await self.data_queue.put(data[offset: offset + length])

    async def close(self):
        await self.data_queue.join()
        self.task.cancel()
        self.streamer.close()
