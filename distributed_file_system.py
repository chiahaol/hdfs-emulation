import asyncio

from config import *
from fs_data_input_stream import FSDataInputStream
from fs_data_output_stream import FSDataOutputStream

class DistributedFileSystem:
    @classmethod
    async def create_instance(cls):
        self = DistributedFileSystem()
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        return self

    def __init__(self):
        pass

    def close(self):
        self.namenode_writer.close()

    def open(self, path):
        return FSDataInputStream()

    async def create(self, path):
        return await FSDataOutputStream.create()
