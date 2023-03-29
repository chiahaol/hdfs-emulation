from block import Block
from config import *


class BlockManager:
    @staticmethod
    def get_file_block_id(filename):
        return int(filename.replace(BLOCK_PREFIX, ""))

    def __init__(self, inode_manager):
        self.im = inode_manager
        self.id_to_block = {}
