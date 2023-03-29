from config import *


class Block:
    def __init__(self, id, locations=None):
        self.id = id
        self.locations = locations if locations != None else []

    def get_filename(self):
        return f'{BLOCK_PREFIX}{"0" * (10 - len(str(self.id)))}{self.id}'