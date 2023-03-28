import asyncio

from config import *

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
       pass

    async def write(self, data_queue):
        while True:
            data = await data_queue.get()
            self.datanode_writer.write(data)
            data_queue.task_done()


    def close(self):
        self.namenode_writer.close()
        self.datanode_writer.close()

