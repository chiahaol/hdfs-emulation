import asyncio
import json
import os
import sys

from config import *
from distributed_file_system import DistributedFileSystem
from fs_data_output_stream import FSDataOutputStream


class EDFSClient:
    @classmethod
    async def create(cls):
        self = EDFSClient()
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        return self

    def __init__(self):
        self.dfs = DistributedFileSystem()

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

    async def rm(self, path):
        if not await self.dfs.exists(path):
            print(f'rm: {path}: No such file or directory')
            return
        elif await self.dfs.is_dir(path):
            print(f'rm: {path}: Is a directory')
            return
        await self.dfs.rm(path)

    async def cat(self, path):
        if not await self.dfs.exists(path):
            print(f'cat: {path}: No such file or directory')
            return
        if await self.dfs.is_dir(path):
            print(f'cat: {path}: Is a directory')
            return

        in_stream = await self.dfs.open(path)
        if not in_stream:
            return
        buf = bytearray([])
        while (await in_stream.read(buf)) > 0:
            if len(buf) >= BUF_LEN:
                print(buf.decode(), end="")
                buf = []
        print(buf.decode(), end="")

        in_stream.close()
        self.dfs.close()

    # TODO: currently only support file types
    # Should implement recursive put all files in a directory in the future
    async def put(self, local_path, remote_path):
        if os.path.basename(remote_path) == "":
            target_path = f'{remote_path}{os.path.basename(local_path)}'
        else:
            target_path = remote_path

        if not os.path.exists(local_path):
            print(f'put: {local_path}: No such file or directory')
            return
        elif await self.dfs.exists(target_path):
            print(f'put: {target_path}: File exists')
            return
        elif not await self.dfs.exists(os.path.dirname(remote_path)):
            print(f'put: {os.path.dirname(remote_path)}: No such file or directory: hdfs://localhost:9000{os.path.dirname(remote_path)}')
            return
        elif not await self.dfs.is_dir(os.path.dirname(remote_path)):
            print(f'put: {os.path.dirname(remote_path)} (is not a directory)')
            return

        out_stream = await self.dfs.create(target_path)
        if not out_stream:
            return
        await out_stream.write(local_path)
        await self.dfs.create_complete(target_path)
        await out_stream.close()
        self.dfs.close()

    # TODO: currently only support file types
    # Should implement recursive get all files in a directory in the future
    async def get(self, remote_path, local_path):
        if os.path.exists(local_path):
            print(f'get: {local_path}: File exists')
            return
        elif not await self.dfs.exists(remote_path):
            print(f'get: {remote_path}: No such file or directory')
            return

        in_stream = await self.dfs.open(remote_path)
        if not in_stream:
            return

        with open(local_path, 'a') as f:
            buf = bytearray([])
            while (await in_stream.read(buf)) > 0:
                if len(buf) >= BUF_LEN:
                    f.write(buf.decode())
                    buf = []
            f.write(buf.decode())

        in_stream.close()
        self.dfs.close()

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
