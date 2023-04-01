import asyncio
import json

from config import *
from fs_data_input_stream import FSDataInputStream
from fs_data_output_stream import FSDataOutputStream

class DistributedFileSystem:
    def __init__(self):
         pass

    def close(self):
        pass

    async def open(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        message = json.dumps({"cmd": CMD_GET_BLOCK_LOCATIONS, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        writer.close()

        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            block_locations = response.get("block_locations")
            return FSDataInputStream(block_locations)

        return None

    async def create(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_CREATE, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        writer.close()

        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            inode_id = response.get("inode_id")
            return FSDataOutputStream(inode_id)

        return None

    async def create_complete(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_CREATE_COMPLETE, "path": path})
        writer.write(message.encode())
        await writer.drain()

        writer.close()

    async def rm(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_RM, "path": path})
        writer.write(message.encode())
        await writer.drain()

        writer.close()

    async def exists(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_FILE_EXISTS, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        exists = response.get("exists")
        return exists

    async def is_dir(self, path):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )

        message = json.dumps({"cmd": CMD_IS_DIR, "path": path})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        exists = response.get("is_dir")
        return exists
