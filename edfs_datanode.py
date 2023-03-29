import asyncio
import json
import os

from block_manager import BlockManager
from config import *
from dfs_packet import DFSPacket


class EDFSDataNode:
    @classmethod
    async def create_instance(cls, ip, port, name):
        self = EDFSDataNode(ip, port, name)
        self.namenode_reader, self.namenode_writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        if not os.path.exists(DATANODE_DATA_DIR):
            os.makedirs(DATANODE_DATA_DIR)

        if not os.path.exists(f'{DATANODE_DATA_DIR}/{self.name}'):
            os.makedirs(f'{DATANODE_DATA_DIR}/{self.name}')
        return self

    def __init__(self, ip, port, name):
        self.ip = ip
        self.port = port
        self.name = name

    async def register(self):
        message = json.dumps({
            "cmd": DN_CMD_REGISTER,
            "ip": self.ip,
            "port": self.port,
            "name": self.name,
            "blocks": self.get_all_block_ids()
        })
        self.namenode_writer.write(message.encode())
        await self.namenode_writer.drain()

        data = await self.namenode_reader.read(BUF_LEN)
        response = json.loads(data.decode())
        success = response.get("success")
        if success:
            print(f'Datanode {self.name} successfully registered')
        else:
            print(response.get("msg"))

    async def serve(self):
        print(f'Datanode {self.name} start serving ...')

        server = await asyncio.start_server(
        self.handle_client, LOCAL_HOST, DATANODE_A_PORT)

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        data = await reader.read(BUF_LEN)
        message = json.loads(data.decode())
        command = message.get("cmd")
        if command == CLI_DATANODE_CMD_SETUP_WRITE:
            await self.setup_write(reader, writer)
            await self.recv_blocks(reader, writer)

        writer.close()

    async def setup_write(self, reader, writer):
        response = {"success": True}
        writer.write(json.dumps(response).encode())
        await writer.drain()
        print(f'DBG: client requested to setup write ')

    async def recv_blocks(self, reader, writer):
        buf = bytearray([])
        while True:
            data = await reader.read(BUF_LEN)
            if not data:
                packet = DFSPacket.decode(buf[:PACKET_BUF_SIZE])
                print(f'{bytes(packet.get_data()).decode()}')
                break

            buf += data
            while len(buf) > PACKET_BUF_SIZE:
                packet = DFSPacket.decode(buf[:PACKET_BUF_SIZE])
                buf = buf[PACKET_BUF_SIZE:]
                print(f'{bytes(packet.get_data()).decode()}')

    def get_all_block_ids(self):
        return [BlockManager.get_file_block_id(filename) for filename in os.listdir(f'{DATANODE_DATA_DIR}/{self.name}') if filename.startswith(BLOCK_PREFIX)]