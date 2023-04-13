import asyncio

from edfs.config import *
from edfs.edfs_namenode import EDFSNameNode

async def main():
    namenode = EDFSNameNode()
    server = await asyncio.start_server(
        namenode.handle_client, LOCAL_HOST, NAMENODE_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
    