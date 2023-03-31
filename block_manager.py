from block import Block
from config import *
from collections import defaultdict


class BlockManager:
    @staticmethod
    def get_file_block_id(filename):
        return int(filename.replace(BLOCK_PREFIX, ""))

    @staticmethod
    def get_filename_from_block_id(block_id):
        return f'{BLOCK_PREFIX}{"0" * (20 - len(str(block_id)))}{block_id}'

    def __init__(self, fsimage):
        self.id_to_block = {}
        self.last_block_id = BLOCK_ID_START
        self.build_blocks(fsimage)

    def build_blocks(self, fsimage):
        inodes = fsimage.get("inodes")
        for inode in inodes:
            inode_id = inode.get("id")
            blocks = inode.get("blocks")
            if not blocks: continue
            for block in blocks:
                blk = Block(block.get("id"), inode_id)
                self.register_block(blk)

    def register_block(self, block):
        self.id_to_block[block.get_id()] = block
        self.last_block_id = max(self.last_block_id, block.get_id())

    def get_block_by_id(self, id):
        return self.id_to_block.get(id)

    def add_block_loc(self, id, datanode_id):
        block = self.get_block_by_id(id)
        block.add_loc(datanode_id)

    def allocate_block_for(self, inode_id):
        blk = Block(self.last_block_id + 1, inode_id)
        self.register_block(blk)
        self.last_block_id += 1
        return blk
