import asyncio
import json
import os

from config import *
from inode import Inode, InodeManager

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
        self.im.print_recursive(self.im.root_inode, 0)

    async def handle_client(self, reader, writer):
        data = await reader.read(BUF_LEN)
        message = json.loads(data.decode())
        command = message["cmd"]
        if command == CMD_LS:
            await self.ls(writer, message["path"])
        elif command == CMD_MKDIR:
            pass
        elif command == CMD_RMDIR:
            pass
        elif command == CMD_TOUCH:
            pass
        elif command == CMD_RM:
            pass
        elif command == CMD_CAT:
            pass
        elif command == CMD_PUT:
            pass
        elif command == CMD_GET:
            pass
        elif command == CMD_TREE:
            await self.tree(writer, message["path"])
        print(f'Client message: {message}')

        # writer.write(b'NameNode successfully received the message!')
        # await writer.drain()
        writer.close()

    async def ls(self, writer, path):
        inode = self.im.get_inode_from_path(path)
        if inode is None:
            response = {"success": False, "msg": f'ls: {path}: No such file or directory'}
            writer.write(json.dumps(response).encode())
            return

        entries = []
        if inode.is_file():
            entries.append(inode.get_path())
        else:
            for dirent in inode.get_dirents():
                if dirent.name != "." and dirent.name != "..":
                    entries.append(dirent.inode.get_path())

        response = {"success": True, "entries": entries}
        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def tree(self, writer, path):
        inode = self.im.get_inode_from_path(path)
        if inode is None:
            response = {"success": False, "msg": f'tree: {path}: No such file or directory'}
            writer.write(json.dumps(response).encode())
            return

        dir_tree = []
        self.tree_helper(inode, 0, dir_tree)
        response = {"success": True, "output": "\n".join(dir_tree)}
        writer.write(json.dumps(response).encode())
        await writer.drain()

    def tree_helper(self, inode, level, output):
        if inode.is_file():
            output.append(f'{"    " * level}{inode.get_name()}')
            return

        output.append(f'{"    " * level}{inode.get_name()}:')
        for dirent in inode.get_dirents():
            if dirent.name != "." and dirent.name != "..":
                self.tree_helper(dirent.inode, level + 1, output)

async def main():
    namenode = NameNode()
    server = await asyncio.start_server(
        namenode.handle_client, LOCAL_HOST, NAMENODE_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
