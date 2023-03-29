import asyncio
import json

from config import *
from dfs_packet import DFSPacket

class DataStreamer:
    @classmethod
    async def create(cls):
        self = DataStreamer()
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        self.datanode_reader, self.datanode_writer = await asyncio.open_connection(
            LOCAL_HOST, DATANODE_A_PORT
        )
        return self

    def __init__(self):
        self.data_queue = asyncio.Queue(MAX_QUEUE_SIZE)
        self.ack_queue = asyncio.Queue(MAX_QUEUE_SIZE)

    async def run(self):
        success = await self.setup_write()
        while True:
            packet = await self.data_queue.get()
            self.datanode_writer.write(DFSPacket.encode(packet))
            await self.datanode_writer.drain()
            self.data_queue.task_done()

    async def finish(self):
        await self.data_queue.join()
        await self.ack_queue.join()

    async def setup_write(self):
        message = json.dumps({"cmd": CLI_DATANODE_CMD_SETUP_WRITE})
        self.datanode_writer.write(message.encode())
        await self.datanode_writer.drain()

        data = await self.datanode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            print(f'DBG: successfully setup write with datanode')
        return success


    async def enqueue(self, item):
       await self.data_queue.put(item)


    def close(self):
        self.namenode_writer.close()
        self.datanode_writer.close()
