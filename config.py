# socket
LOCAL_HOST = '127.0.0.1'
NAMENODE_PORT = 5555
DATANODE_A_PORT = 24000
DATANODE_B_PORT = 25000
DATANODE_C_PORT = 26000
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
CMD_CREATE = 103
CMD_RM = 104
CMD_CAT = 105
CMD_PUT = 106
CMD_GET = 107
CMD_TREE = 108
CMD_ADD_BLOCK = 109

# client to datanode commands
CLI_DATANODE_CMD_SETUP_WRITE = 200
CLI_DATANODE_CMD_WRITE = 201
CLI_DATANODE_CMD_CLOSE = 202

# Datanode to namenode command
DN_CMD_REGISTER = 300

# edit log types
EDIT_TYPE_MKDIR = 400
EDIT_TYPE_RMDIR = 401
EDIT_TYPE_CREATE = 402

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
DEFAULT_BLOCK_SZIE = 1024
BLOCK_ID_START = 2000
BLOCK_PREFIX = "blk_"

# Data Streamer
MAX_QUEUE_SIZE = 4096

# Packet
DEFAULT_PACKET_DATA_SIZE = 256
PACKET_BUF_SIZE = 512

# DataNode
DATANODE_DATA_DIR = "./tmp/data"


# Error code
ERR_FILE_EXIST = "E0"
ERR_FILE_NOT_FOUND = "E1"