import asyncio
import json
import os
import random

from block_manager import BlockManager
from config import *
from datanode_info import DataNodeInfo, DataNodeManager
from editlog_manager import EditLogManager
from inode_manager import InodeManager


class EDFSNameNode:
    def __init__(self):
        fsimage = self.read_fsimage()
        self.im = InodeManager(fsimage)
        self.dnm = DataNodeManager()
        self.bm = BlockManager(fsimage)
        self.elm = EditLogManager(self.im, self.bm)
        self.take_snapshot()

    async def handle_client(self, reader, writer):
        data = await reader.read(BUF_LEN)
        request = json.loads(data.decode())
        command = request.get("cmd")
        if command == CMD_LS:
            await self.ls(writer, request.get("path"))
        elif command == CMD_MKDIR:
            await self.mkdir(writer, request.get("path"))
        elif command == CMD_RMDIR:
            await self.rmdir(writer, request.get("path"))
        elif command == CMD_CREATE:
            await self.create(reader, writer, request)
        elif command == CMD_RM:
            pass
        elif command == CMD_CAT:
            pass
        elif command == CMD_PUT:
            pass
        elif command == CMD_GET:
            pass
        elif command == CMD_TREE:
            await self.tree(writer,request.get("path"))
        elif command == DN_CMD_REGISTER:
            await self.register_datanode(writer, request)
        elif command == CMD_ADD_BLOCK:
            await self.add_block(writer, request)

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
        elif filename == "" or base_dir_inode.get_child_inode_by_name(filename) != None:
            response = {"success": False, "msg": f'mkdir: {path}: File exists'}
        else:
            new_dir_inode = self.im.create_dir(base_dir_inode, filename)
            log = {"edit_type": EDIT_TYPE_MKDIR, "parent": base_dir_inode.get_id(), "name": new_dir_inode.get_name()}
            self.elm.write_log(log)
            response = {"success": True, "msg": f'mkdir: {path}: successfully created with inode number {new_dir_inode.get_id()}'}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def rmdir(self, writer, path):
        inode = self.im.get_inode_from_path(path)
        if inode == self.im.root_inode:
            response = {"success": False, "msg": f'rmdir: Can not remvoe the toot directory'}
        elif inode is None:
            response = {"success": False, "msg": f'rmdir: {path}: No such file or directory'}
        elif inode.is_file():
            response = {"success": False, "msg": f'rmdir: {path}: Is not a directory'}
        elif len(inode.get_dirents()) > 2:
            response = {"success": False, "msg": f'rmdir: {path}: Directory is not empty'}
        else:
            parent = inode.get_parent_inode()
            self.im.remove_dir(parent, inode)
            log = {"edit_type": EDIT_TYPE_RMDIR, "parent": parent.get_id(), "remove": inode.get_id(), "name": inode.get_name()}
            self.elm.write_log(log)
            response = {"success": True, "msg": f'rmdir: {path}: successfully removed the directory'}

        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def create(self, reader, writer, request):
        path = request.get("path")
        base_dir_inode, filename = self.im.get_baseinode_and_filename(path)
        if base_dir_inode is None:
            response = {"success": False, "error": ERR_FILE_NOT_FOUND}
        elif filename == "" or base_dir_inode.get_child_inode_by_name(filename) != None:
            response = {"success": False, "error": ERR_FILE_EXIST}
        else:
            new_file_inode = self.im.create_file(base_dir_inode, filename)
            log = {"edit_type": EDIT_TYPE_CREATE, "parent": base_dir_inode.get_id(), "name": new_file_inode.get_name()}
            self.elm.write_log(log)
            response = {"success": True, "inode_id": new_file_inode.get_id()}

        writer.write(json.dumps(response).encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        request = json.loads(data.decode())
        if request.get("cmd") == CMD_CREATE_DONE:
            print(f'DBG: Finish creating {path}')

        return

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

    def take_snapshot(self):
        self.elm.process_edit_logs()

        inodes = []
        directories = []
        for inode in self.im.get_all_inodes():
            if inode.is_file():
                blocks = []
                for block_id in inode.get_blocks():
                    blk = self.bm.get_block_by_id(block_id)
                    blocks.append({"id":  blk.get_id(), "numBytes": blk.get_num_bytes()})

                inodes.append({
                    "id": inode.get_id(),
                    "type": inode.get_type(),
                    "name":  inode.get_name(),
                    "replication": inode.get_replication(),
                    "preferredBlockSize": inode.get_preferredBlockSize(),
                    "blocks": blocks
                })
            else:
                inodes.append({
                    "id": inode.get_id(),
                    "type": inode.get_type(),
                    "name":  inode.get_name(),
                })

                children_ids = inode.get_children_ids()
                if children_ids:
                    directories.append({
                        "parent": inode.get_id(),
                        "children": children_ids
                    })
        fsimage = {"inodes": inodes, "directories": directories}
        with open(f'{NAMENODE_METADATA_DIR}/{FSIMAGE_FILENAME}', 'w') as f:
            json.dump(fsimage, f, indent=2)

        self.elm.remove_edit_logs()

    async def register_datanode(self, writer, request):
        datanode_info = DataNodeInfo(request.get("ip"), request.get("port"), request.get("name"))
        self.dnm.register(datanode_info)
        blocks = request.get("blocks")
        for block_id in blocks:
            self.bm.add_block_loc(block_id, datanode_info.get_id())
        response = {
            "success": True,
            "msg": f'Datanode {datanode_info.get_id()} ({datanode_info.get_name()}): {datanode_info.get_ip()}:{datanode_info.get_port()}'
        }
        writer.write(json.dumps(response).encode())
        await writer.drain()

    async def add_block(self, writer, request):
        inode_id = request.get("inode_id")
        blk = self.bm.allocate_block_for(inode_id)
        self.im.add_block_to(inode_id, blk.get_id())
        blk_locs = self.select_block_locs(REPLICATION_FACTOR)
        blk_locs_info = [
            {
                "ip": datanode_info.get_ip(),
                "port": datanode_info.get_port(),
                "name": datanode_info.get_name()
            }
            for datanode_info in blk_locs
        ]
        log = {"edit_type": EDIT_TYPE_ADD_BLOCK, "inode_id": inode_id, "block_id": blk.get_id()}
        self.elm.write_log(log)
        response = {"success": True, "inode_id": inode_id, "block_id": blk.get_id(), "blk_locs_info": blk_locs_info}

        writer.write(json.dumps(response).encode())
        await writer.drain()
        return

    # TODO: for simplicity, we randomly choose from all datanodes
    def select_block_locs(self, num):
        datanodes = self.dnm.get_all_datanodes()
        return random.sample(datanodes, num)


async def main():
    namenode = EDFSNameNode()
    server = await asyncio.start_server(
        namenode.handle_client, LOCAL_HOST, NAMENODE_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
