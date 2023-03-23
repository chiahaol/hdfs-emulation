import asyncio

from config import *

async def handle_client(reader, writer):
    data = await reader.read(100)
    message = data.decode()

    print(f'Client message: {message}')

    writer.write(b'NameNode successfully received the message!')
    await writer.drain()
    writer.close()

async def main():
    server = await asyncio.start_server(
        handle_client, LOCAL_HOST, NAMENODE_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
