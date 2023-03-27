# socket
LOCAL_HOST = '127.0.0.1'
NAMENODE_PORT = 5555
BUF_LEN = 65536

# client program command line commands
CLI_LS = "-ls"
CLI_MKDIR = "-mkdir"
CLI_RMDIR = "-rmdir"
CLI_TOUCH = "-touch"
CLI_RM = "-rm"
CLI_CAT = "-cat"
CLI_PUT = "-put"
CLI_GET = "-get"
CLI_TREE = "-tree"

# client to namenode commands
CMD_LS = 100
CMD_MKDIR = 101
CMD_RMDIR = 102
CMD_TOUCH = 103
CMD_RM = 104
CMD_CAT = 105
CMD_PUT = 106
CMD_GET = 107
CMD_TREE = 108

# edit log types
EDIT_TYPE_MKDIR = 200
EDIT_TYPE_RMDIR = 201

# Metadata
NAMENODE_METADATA_DIR = "./tmp/name"
FSIMAGE_FILENAME = "fsimage.json"

EDIT_LOG_PREFIX = "edits_"

#Inode
DIR_TYPE = "DIRECTORY"
FILE_TYPE = "FILE"
ROOT_DIR_NAME = ""
DEFAULT_BASE_DIR = "/user/ubuntu"

INODE_ID_START = 1000


# Block
BLOCK_ID_START = 2000
