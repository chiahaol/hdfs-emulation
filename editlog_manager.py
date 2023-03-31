import json
import os

from block import Block
from config import *

class EditLogManager:
    def __init__(self, inode_manager, block_manager):
        self.last_edit_log_id = 0
        self.im = inode_manager
        self.bm = block_manager

    def write_log(self, log):
        with open(self.get_next_edit_log_filename(), 'w') as output:
                json.dump(log, output)

    def process_edit_logs(self):
        edit_log_filenames = [filename for filename in os.listdir(NAMENODE_METADATA_DIR) if filename.startswith(EDIT_LOG_PREFIX)]
        edit_log_filenames.sort()
        for filename in edit_log_filenames:
            with open(f'{NAMENODE_METADATA_DIR}/{filename}', 'r') as f:
                log = json.load(f)

            edit_type = log.get("edit_type")
            if edit_type == EDIT_TYPE_MKDIR:
                self.process_mkdir_editlog(log)
            elif edit_type == EDIT_TYPE_RMDIR:
                self.process_rmdir_editlog(log)
            elif edit_type == EDIT_TYPE_CREATE:
                self.process_create_editlog(log)
            elif edit_type == EDIT_TYPE_ADD_BLOCK:
                self.process_add_block_editlog(log)

    def process_mkdir_editlog(self, log):
        parent_id,  name = log.get("parent"), log.get("name")
        parent_inode = self.im.id_to_inode[parent_id]
        self.im.create_dir(parent_inode, name)

    def process_rmdir_editlog(self, log):
        parent_id, remove_id, name = log.get("parent"), log.get("remove"), log.get("name")
        parent_inode = self.im.id_to_inode[parent_id]
        remove_inode = self.im.id_to_inode[remove_id]
        self.im.remove_dir(parent_inode, remove_inode)

    def process_create_editlog(self, log):
        parent_id, name = log.get("parent"), log.get("name")
        parent_inode = self.im.id_to_inode[parent_id]
        self.im.create_file(parent_inode, name)

    def process_add_block_editlog(self, log):
        inode_id, block_id = log.get("inode_id"), log.get("block_id")
        blk = Block(block_id, inode_id)
        self.bm.register_block(blk)
        self.im.add_block_to(inode_id, block_id)

    def get_next_edit_log_filename(self):
        self.last_edit_log_id += 1
        return f'{NAMENODE_METADATA_DIR}/{EDIT_LOG_PREFIX}{"0" * (8 - len(str(self.last_edit_log_id)))}{self.last_edit_log_id}'

    def remove_edit_logs(self):
        edit_log_filenames = [filename for filename in os.listdir(NAMENODE_METADATA_DIR) if filename.startswith(EDIT_LOG_PREFIX)]
        for filename in edit_log_filenames:
            os.remove(f'{NAMENODE_METADATA_DIR}/{filename}')