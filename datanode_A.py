import asyncio
import json
import os

from config import *
from edfs_datanode import EDFSDataNode

async def main():
    datanode = EDFSDataNode()
    server = await asyncio.start_server(
        datanode.handle_client, LOCAL_HOST, DATANODE_A_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())