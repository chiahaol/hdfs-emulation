import asyncio
import json
import os

from config import *
from inode import InodeManager

class MetaData:
    def __init__(self):
        self.fsimage = self.read_fsimage();

    def read_fsimage(self):
        if not os.path.exists(NAMENODE_METADATA_DIR):
            os.makedirs(NAMENODE_METADATA_DIR)

        if not os.path.exists(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}'):
            with open(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}', 'w') as f:
                fsimage = {"inodes": [{"id": INODE_ID_START, "type": "DIRECTORY", "name": ""}], "directories": []}
                json.dump(fsimage, f)
        else:
            with open(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}', 'r') as f:
                fsimage = json.load(f)

        return fsimage


class NameNode:
    def __init__(self):
        self.metadata = MetaData()
        self.im = InodeManager()
        self.im.build_inodes(self.metadata.fsimage)
        # self.im.print_recursive(self.im.root_inode, 0)

    async def handle_client(self, reader, writer):
        data = await reader.read(100)
        message = data.decode()
        print(f'Client message: {message}')

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
