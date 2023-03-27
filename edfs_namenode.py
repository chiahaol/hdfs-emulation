import asyncio
import json
import os

from config import *
from inode import Inode, InodeManager


class NameNode:
    def __init__(self):
        self.last_edit_log_id = 0
        self.fsimage = self.read_fsimage();
        self.process_edit_logs()
        self.take_screenshot()
        self.remove_edit_logs()
        self.im = InodeManager()
        self.im.build_inodes(self.fsimage)

    async def handle_client(self, reader, writer):
        data = await reader.read(BUF_LEN)
        message = json.loads(data.decode())
        command = message.get("cmd")
        if command == CMD_LS:
            await self.ls(writer, message.get("path"))
        elif command == CMD_MKDIR:
            await self.mkdir(writer, message.get("path"))
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

    async def mkdir(self, writer, path):
        base_dir_inode, filename = self.im.get_baseinode_and_filename(path)
        if base_dir_inode is None:
            response = {"success": False, "msg": f'mkdir: {os.path.dirname(path.strip(" /"))}: No such file or directory'}
            writer.write(json.dumps(response).encode())
            return
        elif filename == "" or base_dir_inode.get_child_inode_by_name(filename) != None:
            response = {"success": False, "msg": f'mkdir: {path}: File exists'}
            writer.write(json.dumps(response).encode())
            return

        new_dir_inode = self.im.create_dir(base_dir_inode, filename)
        log = {"edit_type": EDIT_TYPE_MKDIR, "parent": base_dir_inode.get_id(), "child": new_dir_inode.get_id(), "name": new_dir_inode.get_name()}
        with open(self.get_next_edit_log_filename(), 'w') as output:
            json.dump(log, output)

        response = {"success": True, "msg": f'mkdir: {path}: successfully created with inode number {new_dir_inode.get_id()}'}
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

    def process_edit_logs(self):
        edit_log_filenames = [filename for filename in os.listdir(NAMENODE_METADATA_DIR) if filename.startswith(EDIT_LOG_PREFIX)]
        edit_log_filenames.sort()
        for filename in edit_log_filenames:
            with open(f'{NAMENODE_METADATA_DIR}/{filename}', 'r') as f:
                log = json.load(f)

            edit_type = log.get("edit_type")
            if edit_type == EDIT_TYPE_MKDIR:
                parent, child, name = log.get("parent"), log.get("child"), log.get("name")
                self.fsimage.get("inodes").append({
                    "id": child,
                    "type": DIR_TYPE,
                    "name": name
                })
                parent_dir_obj = None
                for obj in self.fsimage.get("directories"):
                    if obj.get("parent") == parent:
                        parent_dir_obj = obj
                        break
                if parent_dir_obj:
                    parent_dir_obj.get("children").append(child)
                else:
                    self.fsimage.get("directories").append({"parent": parent, "children": [child]})

    def get_next_edit_log_filename(self):
        self.last_edit_log_id += 1
        return f'{NAMENODE_METADATA_DIR}/{EDIT_LOG_PREFIX}{"0" * (8 - len(str(self.last_edit_log_id)))}{self.last_edit_log_id}'

    def remove_edit_logs(self):
        edit_log_filenames = [filename for filename in os.listdir(NAMENODE_METADATA_DIR) if filename.startswith(EDIT_LOG_PREFIX)]
        for filename in edit_log_filenames:
            os.remove(f'{NAMENODE_METADATA_DIR}/{filename}')

    def take_screenshot(self):
        with open(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}', 'w') as f:
            json.dump(self.fsimage, f, indent=2)


async def main():
    namenode = NameNode()
    server = await asyncio.start_server(
        namenode.handle_client, LOCAL_HOST, NAMENODE_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
