# more details at:
#   https://docs.python.org/3/library/asyncio-stream.html#examples

import asyncio
import json
import sys

from config import *


async def main():
    reader, writer = await asyncio.open_connection(
        LOCAL_HOST, NAMENODE_PORT
    )

    if (len(sys.argv) < 2):
        print("Please provide a edfs command!")
        exit(-1)

    command = sys.argv[1]
    if command == CLI_LS:
        await ls(reader, writer)
    elif command == CLI_MKDIR:
        await mkdir(reader, writer)
    elif command == CLI_RMDIR:
        await rmdir(reader, writer)
    elif command == CLI_TOUCH:
        await touch(reader, writer)
    elif command == CLI_RM:
        await rm(reader, writer)
    elif command == CLI_CAT:
        await cat(reader, writer)
    elif command == CLI_PUT:
        await put(reader, writer)
    elif command == CLI_GET:
        await get(reader, writer)
    elif command == CLI_TREE:
        await tree(reader, writer)
    else:
        print(f'Command not found: {command}')
        exit(-1)

    writer.close()

async def ls(reader, writer):
    path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_BASE_DIR
    message = json.dumps({"cmd": CMD_LS, "path": path})
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(BUF_LEN)
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

async def mkdir(reader, writer):
    path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_BASE_DIR
    message = json.dumps({"cmd": CMD_MKDIR, "path": path})
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(BUF_LEN)
    response = json.loads(data.decode())
    success = response.get("success")
    if not success:
        print(response.get("msg"))
    else:
        print(response.get("msg"))

async def rmdir(reader, writer):
    message = "rmdir"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

async def touch(reader, writer):
    message = "touch"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

async def rm(reader, writer):
    message = "rm"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

async def cat(reader, writer):
    message = "cat"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

async def put(reader, writer):
    message = "put"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

async def get(reader, writer):
    message = "get"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

async def tree(reader, writer):
    path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_BASE_DIR
    message = json.dumps({"cmd": CMD_TREE, "path": path})
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(BUF_LEN)
    response = json.loads(data.decode())
    success = response.get("success")
    if not success:
        print(response.get("msg"))
    else:
        dir_tree = response.get("output")
        print(dir_tree)

if __name__ == "__main__":
    asyncio.run(main())
