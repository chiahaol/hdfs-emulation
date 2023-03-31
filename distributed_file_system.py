import asyncio
import json

from config import *
from fs_data_input_stream import FSDataInputStream
from fs_data_output_stream import FSDataOutputStream

class DistributedFileSystem:
    @classmethod
    async def create_instance(cls):
        self = DistributedFileSystem()
        return self

    def __init__(self):
         self.namenode_reader = None
         self.namenode_writer = None

    def close(self):
        if self.namenode_writer:
            self.namenode_writer.close()

    def open(self, path):
        return FSDataInputStream()

    async def create(self, path):
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_CREATE, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            inode_id = response.get("inode_id")
            return inode_id

        # TODO: propagate error to client program
        error = response.get("error")
        if error == ERR_FILE_NOT_FOUND:
            print(f'{path}: No such file or directory: hdfs://localhost:9000/{path}')
        elif error == ERR_FILE_EXIST:
            print(f'{path}: File exists')

        return None

    async def create_done(self, path):
        message = json.dumps({"cmd": CMD_CREATE_DONE, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()
