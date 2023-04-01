import asyncio
import json
import os

from config import *
from edfs_datanode import EDFSDataNode

async def main():
    datanode = await EDFSDataNode.create_instance(LOCAL_HOST, DATANODE_C_PORT, "C")
    await datanode.register()
    await datanode.serve()

if __name__ == "__main__":
    asyncio.run(main())