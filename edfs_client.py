import asyncio
import json
import sys

from config import *
from distributed_file_system import DistributedFileSystem


class EDFSClient:
    @classmethod
    async def create(cls):
        self = EDFSClient()
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        self.dfs = await DistributedFileSystem.create_instance()
        return self

    def __init__(self):
        pass

    def close(self):
        if self.namenode_writer:
            self.namenode_writer.close()
        self.dfs.close()

    async def ls(self, path):
        message = json.dumps({"cmd": CMD_LS, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await  self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if not success:
            print(response.get("msg"))
        else:
            entries = response.get("entries")
            if not entries:
                return

            print(f'Found {len(entries)} items')
            for ent in entries:
                print(ent)

    async def mkdir(self, path):
        message = json.dumps({"cmd": CMD_MKDIR, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if not success:
            print(response.get("msg"))

    async def rmdir(self, path):
        message = json.dumps({"cmd": CMD_RMDIR, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if not success:
            print(response.get("msg"))

    async def touch(self):
        message = "touch"
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(100)
        print(f'{data.decode()!r}')

    async def rm(self):
        message = "rm"
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(100)
        print(f'{data.decode()!r}')

    async def cat(self):
        message = "cat"
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(100)
        print(f'{data.decode()!r}')

    async def put(self, local_path, remote_path):
        out_stream = await self.dfs.create(remote_path)
        out_stream.open()
        with open(local_path, 'r') as f:
            while True:
                data = f.read(DEFAULT_FILE_PACKET_SIZE).encode()
                if len(data) == 0: break
                await out_stream.write(data, 0, len(data))
        await out_stream.close()

    async def get(self):
        message = "get"
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(100)
        print(f'{data.decode()!r}')

    async def tree(self, path):
        message = json.dumps({"cmd": CMD_TREE, "path": path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if not success:
            print(response.get("msg"))
        else:
            dir_tree = response.get("output")
            print(dir_tree)
