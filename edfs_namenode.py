import asyncio

from config import *

class NameNode:
    def __init__(self):
        self.count = 0

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        self.count += 1
        print(f'Client message: {message} ({self.count})')

        writer.write(b'NameNode successfully received the message!')
        await writer.drain()
        writer.close()

async def main():
    namenode = NameNode()
    server = await asyncio.start_server(
        namenode.handle_client, LOCAL_HOST, NAMENODE_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
