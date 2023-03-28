import asyncio
import json
import sys

from config import *


class EDFSClient:
    def __init__(self):
        self.namenode_reader = None
        self.namenode_writer = None

    async def connect_namenode(self, ip=LOCAL_HOST, port=NAMENODE_PORT):
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            ip, port
        )

    def close(self):
        if self.namenode_writer:
            self.namenode_writer.close()

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

    async def put(self):
        if len(sys.argv) < 3:
            print("-put: Not enough arguments: expected 1 but got 0")
            exit(-1)

        local_path = sys.argv[2]
        remote_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_BASE_DIR
        message = json.dumps({"cmd": CMD_PUT, "local_path": local_path, "remote_path": remote_path})
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        print(response)

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
