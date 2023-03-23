# more details at:
#   https://docs.python.org/3/library/asyncio-stream.html#examples

import asyncio, sys

from config import *

async def main():
    reader, writer = await asyncio.open_connection(
        LOCAL_HOST, NAMENODE_PORT
    )

    while True:
        message = input()
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(100)
        print(f'{data.decode()!r}')

    writer.close()


if __name__ == "__main__":
    asyncio.run(main())
