import asyncio
import json
import os

from config import *


class EDFSDataNode:
    def __init__(self):
        pass

    async def handle_client(self, reader, writer):
        while True:
            data = await reader.read(BUF_LEN)
            if len(data) == 0: break
            print(data.decode())
            # message = json.loads(data.decode())
            # command = message.get("cmd")
            # if command == CLI_DATANODE_CMD_SETUP:
            #     pass
            # elif command == CLI_DATANODE_CMD_WRITE:
            #     content = message.get("content")
            #     print(content)
            # elif command == CLI_DATANODE_CMD_CLOSE:
            #     break

        writer.close()