# more details at:
#   https://docs.python.org/3/library/asyncio-stream.html#examples

import asyncio
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
    if command == CMD_LS:
        await ls(reader, writer)
    elif command == CMD_MKDIR:
        await mkdir(reader, writer)
    elif command == CMD_RMDIR:
        await rmdir(reader, writer)
    elif command == CMD_TOUCH:
        await touch(reader, writer)
    elif command == CMD_RM:
        await rm(reader, writer)
    elif command == CMD_CAT:
        await cat(reader, writer)
    elif command == CMD_PUT:
        await put(reader, writer)
    elif command == CMD_GET:
        await get(reader, writer)
    else:
        print(f'Command not found: {command}')
        exit(-1)

    writer.close()

async def ls(reader, writer):
    message = "ls"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

async def mkdir(reader, writer):
    message = "mkdir"
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    print(f'{data.decode()!r}')

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


if __name__ == "__main__":
    asyncio.run(main())
