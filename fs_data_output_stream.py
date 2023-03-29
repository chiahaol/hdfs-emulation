import asyncio

from collections import deque
from config import *
from data_streamer import DataStreamer
from dfs_packet import DFSPacket

class FSDataOutputStream:
    @classmethod
    async def create(cls):
        self = FSDataOutputStream()
        self.streamer = await DataStreamer.create()
        self.task = None
        return self

    def __init__(self):
        pass

    def get_streamer(self):
        return self.streamer

    async def enqueue_packet(self, packet):
        await self.get_streamer().enqueue(packet)

    async def wait_for_streamer(self):
        await self.get_streamer().finish()

    async def write_file(self, filepath):
        self.task = asyncio.create_task(self.streamer.run())
        block_capacity = DEFAULT_BLOCK_SZIE
        with open(filepath, 'r') as f:
            while True:
                data = f.read(min(DEFAULT_PACKET_DATA_SIZE, block_capacity)).encode()
                if len(data) == 0: break
                block_capacity -= len(data)
                if block_capacity == 0:
                    packet = DFSPacket.create_packet(data, True)
                    block_capacity = DEFAULT_BLOCK_SZIE
                else:
                    packet = DFSPacket.create_packet(data, False)
                await self.enqueue_packet(packet)

    async def close(self):
        await self.wait_for_streamer()
        if self.task != None:
            self.task.cancel()
        self.streamer.close()
