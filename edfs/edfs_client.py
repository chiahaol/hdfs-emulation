import asyncio
import json
import os
import sys

from edfs.config import *
from edfs.distributed_file_system import DistributedFileSystem

class EDFSClient:
    def __init__(self):
        self.dfs = DistributedFileSystem()

    def close(self):
        self.dfs.close()

    async def ls(self, path):
        files = await self.dfs.listdir(path)
        if files is None:
            print(f'ls: {path}: No such file or directory')
            return

        if files:
            print(f'Found {len(files)} items')
        for file in files:
            file_type = file.get("type")
            num_bytes = str(file.get("numBytes"))
            file_path = file.get("path")
            if file_type == DIR_TYPE:
                print(f'{file_type}{" " * 10}{num_bytes} {file_path}')
            else:
                print(f'{file_type}{" " * (16 - len(num_bytes))}{num_bytes} {file_path}')

    async def mkdir(self, path):
        if await self.dfs.exists(path):
            print(f'mkdir: {path}: File exists')
            return
        elif not await self.dfs.exists(os.path.dirname(path.strip(" /"))):
            print(f'mkdir: {os.path.dirname(path.strip(" /"))}: No such file or directory')
            return

        await self.dfs.mkdir(path)
        self.dfs.close()

    async def rmdir(self, path):
        if await self.dfs.is_root_dir(path):
            print(f'rmdir: Can not remvoe the root directory')
            return
        elif not await self.dfs.exists(path):
            print(f'rmdir: {path}: No such file or directory')
            return
        elif not await self.dfs.is_dir(path):
            print(f'rmdir: {path}: Is not a directory')
            return
        elif not await self.dfs.is_dir_empty(path):
            print(f'rmdir: {path}: Directory is not empty')
            return

        await self.dfs.rmdir(path)
        self.dfs.close()

    async def touch(self, path):
        if not await self.dfs.exists(os.path.dirname(path)):
            print(f'touch: {os.path.dirname(path)}: No such file or directory')
            return
        elif await self.dfs.is_dir(path):
            print(f'touch: {path}: Is a directory')
            return
        elif await self.dfs.exists(path):
            return

        out_stream = await self.dfs.create(path)
        await out_stream.close()
        self.dfs.close()

    async def rm(self, path):
        if not await self.dfs.exists(path):
            print(f'rm: {path}: No such file or directory')
            return
        elif await self.dfs.is_dir(path):
            print(f'rm: {path}: Is a directory')
            return
        await self.dfs.rm(path)

    async def cat(self, path):
        if not await self.dfs.exists(path):
            print(f'cat: {path}: No such file or directory')
            return
        if await self.dfs.is_dir(path):
            print(f'cat: {path}: Is a directory')
            return

        in_stream = await self.dfs.open(path)
        if not in_stream:
            return
        buf = bytearray([])
        while (await in_stream.read(buf)) >= 0:
            if len(buf) >= BUF_LEN:
                print(buf.decode(), end="")
                buf = bytearray([])
        print(buf.decode(), end="")

        in_stream.close()
        self.dfs.close()

    # TODO: currently only support file types
    # Should implement recursive put all files in a directory in the future
    async def put(self, local_path, remote_path):
        if os.path.basename(remote_path) == "" or await self.dfs.is_dir(remote_path):
            target_path = f'{remote_path}/{os.path.basename(local_path)}'
        else:
            target_path = remote_path

        if not os.path.exists(local_path):
            print(f'put: {local_path}: No such file or directory')
            return False
        elif await self.dfs.exists(target_path):
            print(f'put: {target_path}: File exists')
            return False
        elif not await self.dfs.exists(os.path.dirname(remote_path)):
            print(f'put: {os.path.dirname(remote_path)}: No such file or directory: hdfs://localhost:9000{os.path.dirname(remote_path)}')
            return False
        elif not await self.dfs.is_dir(os.path.dirname(remote_path)):
            print(f'put: {os.path.dirname(remote_path)} (is not a directory)')
            return False

        out_stream = await self.dfs.create(target_path)
        if not out_stream:
            return False
        await out_stream.write(local_path)
        await self.dfs.create_complete(target_path)
        await out_stream.close()
        self.dfs.close()

        return True

    # TODO: currently only support file types
    # Should implement recursive get all files in a directory in the future
    async def get(self, remote_path, local_path):
        target_path = local_path
        if os.path.isdir(local_path):
            target_path = f'{local_path}/{os.path.basename(remote_path.strip(" /"))}'

        if os.path.exists(target_path):
            print(f'get: {target_path}: File exists')
            return
        if not await self.dfs.exists(remote_path):
            print(f'get: {remote_path}: No such file or directory')
            return
        if await self.dfs.is_dir(remote_path):
            print(f'get: {remote_path}: Is a directory')
            return

        in_stream = await self.dfs.open(remote_path)
        if not in_stream:
            return

        with open(target_path, 'a') as f:
            buf = bytearray([])
            while (await in_stream.read(buf)) > 0:
                if len(buf) >= BUF_LEN:
                    f.write(buf.decode())
                    buf = bytearray([])
            f.write(buf.decode())

        in_stream.close()
        self.dfs.close()

    async def get_file(self, path):
        if not await self.dfs.exists(path):
            return {"success": False}
        if await self.dfs.is_dir(path):
            return {"success": False}

        in_stream = await self.dfs.open(path)
        if not in_stream:
            return {"success": False}

        buf = bytearray([])
        while (await in_stream.read(buf)) > 0:
            continue

        in_stream.close()
        self.dfs.close()

        return {"success": True, "file": buf.decode()}

    async def get_block(self, blk_name):
        in_stream = await self.dfs.open_block(blk_name)
        if not in_stream:
            return {"success": False}

        buf = bytearray([])
        while (await in_stream.read(buf)) > 0:
            continue

        in_stream.close()
        self.dfs.close()

        return {"success": True, "block": buf.decode()}


    async def mv(self, src, des):
        if not await self.dfs.exists(src):
            print(f'mv: {src}: No such file or directory')
            return

        if not await self.dfs.exists(des):
            dirname = os.path.dirname(des.rstrip("/"))
            if not await self.dfs.exists(dirname) or not await self.dfs.is_dir(dirname):
                print(f'mv: {dirname}: No such file or directory: edfs://localhost:9000{dirname}')
                return
            target = des
        else:
            if await self.dfs.is_dir(des):
                target = f'{des}/{os.path.basename(src.rstrip("/"))}'
                if await self.dfs.is_identical(src, target):
                    print(f'mv: {src} to edfs://localhost:9000{target}: are identical')
                    return
                elif await self.dfs.exists(target):
                    print(f'mv: {target}: File exists')
                    return
                elif await self.dfs.is_identical(src, des):
                    print(f'mv: {src} to edfs://localhost:9000{des}: is a subdirectory of itself')
                    return
            else:
                print(f'mv: {des}: File exists')
                return

        await self.dfs.mv(src, target)

    async def tree(self, path):
        files = await self.dfs.listdir_recursive(path)
        if files is None:
            print(f'ls: {path}: No such file or directory')
            return

        output = []
        self.tree_helper(files, 0, output)
        print("\n".join(output))

    def tree_helper(self, files, level, output):
        for file in files:
            filename = file.get("name")
            file_type = file.get("type")

            if file_type == FILE_TYPE:
                output.append(f'{"    " * level}{filename}')
            else:
                output.append(f'{"    " * level}{filename}:')
                children = file.get("children")
                self.tree_helper(children, level + 1, output)

    async def get_all_files(self):
        return await self.dfs.listdir_recursive()

    async def get_file_blk_names(self, filepath):
        return await self.dfs.get_file_blocks(filepath)
