import asyncio
import json
import os

from edfs.config import *
from edfs.edfs_datanode import EDFSDataNode

async def main():
    datanode = await EDFSDataNode.create_instance(LOCAL_HOST, DATANODE_B_PORT, "B")
    await datanode.register()
    await datanode.serve()

if __name__ == "__main__":
    asyncio.run(main())