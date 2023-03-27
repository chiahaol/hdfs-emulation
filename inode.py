from config import *

class Inode:
    def __init__(self, id, type, name, replication=0, preferredBlockSize=0, blocks=None):
        self.id = id
        self.type = type
        self.name = name
        self.replication = replication
        self.preferredBlockSize = preferredBlockSize
        self.blocks = blocks if blocks is not None else []
        self.dir_entries = [DirEnt(self, ".")]

    def is_dir(self):
        return self.type == DIR_TYPE

    def is_file(self):
        return self.type == FILE_TYPE

    def get_id(self):
        return self.id

    def get_name(self):
        return self.name

    def add_dirent(self, inode, name):
        self.dir_entries.append(DirEnt(inode, name))

    def get_parent(self):
        for dirent in self.dir_entries:
            if dirent.name == "..":
                return dirent.inode

    def get_path(self):
        cur = self
        parent = cur.get_parent()
        path = [cur.get_name()]
        while parent != cur:
            cur = parent
            path.append(cur.get_name())
            parent = cur.get_parent()
        return "/".join(path[::-1])

class DirEnt:
    def __init__(self, inode, name):
        self.inode = inode
        self.name = name

class InodeManager():
    def __init__(self):
        self.last_inode_id = INODE_ID_START
        self.id_to_inode = {}
        self.root_inode = None

    def build_inodes(self, metadata):
        inodes = metadata["inodes"]
        directories = metadata["directories"]

        for inode in inodes:
            if inode["type"] == DIR_TYPE:
                node = Inode(inode["id"], inode["type"], inode["name"])
            else:
                node = Inode(inode["id"], inode["type"], inode["name"], inode["replication"], inode["preferredBlockSize"], inode.get("blocks"))
            self.id_to_inode[node.get_id()] = node
            if node.get_name() == ROOT_DIR_NAME:
                self.root_inode = node
            self.last_inode_id = max(self.last_inode_id, node.get_id())

        for directory in directories:
            parent_id = directory["parent"]
            children_ids = directory["children"]

            p_node = self.id_to_inode[parent_id]
            for child_id in children_ids:
                c_node = self.id_to_inode[child_id]
                p_node.add_dirent(c_node, c_node.get_name())
                c_node.add_dirent(p_node, "..")

        self.root_inode.add_dirent(self.root_inode, "..")

    def print_entries(self, dir_inode):
        dir_path = dir_inode.get_path()
        for dirent in  dir_inode.dir_entries:
            if dirent.name != "." and dirent.name != "..":
                print(f'{dir_path}/{dirent.name}')
        print()

    def print_recursive(self, inode, level):
        if inode.is_file():
            print(f'{"    " * level}{inode.get_name()}')
            return

        print(f'{"    " * level}{inode.get_name()}:')
        for dirent in inode.dir_entries:
            if dirent.name != "." and dirent.name != "..":
                self.print_recursive(dirent.inode, level + 1)
