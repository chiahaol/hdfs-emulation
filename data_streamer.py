import asyncio
import json

from block import Block, LocatedBlock
from config import *
from dfs_packet import DFSPacket
from utils import PacketUtils

class DataStreamer:
    @classmethod
    async def create(cls):
        self = DataStreamer()
        return self

    def __init__(self):
        self.des_inode_id = None
        self.task = None
        self.data_queue = asyncio.Queue(MAX_QUEUE_SIZE)
        self.ack_queue = asyncio.Queue(MAX_QUEUE_SIZE)
        self.reader = None
        self.writer = None

    def setup(self, task, des_inode_id):
        self.task = task
        self.des_inode_id = des_inode_id

    async def enqueue(self, item):
       await self.data_queue.put(item)

    async def run(self):
        block_id = None
        blk_locs_info = None
        ack_task = None
        should_allocate_new_block = True
        while True:
            if should_allocate_new_block:
                block_id, blk_locs_info = await self.request_new_block()
                target = blk_locs_info.pop(0)
                self.reader, self.writer = await self.setup_pipeline(block_id, target, blk_locs_info)
                ack_task = asyncio.create_task(self.recv_acks(self.reader))
                should_allocate_new_block = False
            packet = await self.data_queue.get()
            await self.write_to_pipeline(self.writer, packet, block_id, blk_locs_info)
            await self.ack_queue.put(packet)
            self.data_queue.task_done()
            if packet.is_last_packet_in_block():
                await self.wait_for_all_ack()
                ack_task.cancel()
                print(f'DBG: block {block_id} was successfully sent to datanodes {target.get("name")} {" ".join([loc.get("name") for loc in blk_locs_info])}')
                should_allocate_new_block = True
                self.writer.close()

    async def recv_acks(self, nextnode_reader):
        buf = bytearray([])
        while True:
            data = await nextnode_reader.read(BUF_LEN)
            if not data:
                break
            buf += data

            packets, ptr = PacketUtils.create_packets_from_buffer(buf)
            decoded_packets = [json.loads(packet.decode()) for packet in packets]
            seqnos = [packet.get("seqno") for packet in decoded_packets]
            for seqno in seqnos:
                packet = await self.ack_queue.get()
                print(f'DBG: received ack {seqno}, packet {packet.get_seqno()} popped from ack queue')
                self.ack_queue.task_done()
            buf = buf[ptr:]

    async def write_to_pipeline(self, writer, packet, block_id, next_datanodes):
        buf = {
            "seqno": packet.get_seqno(),
            "data": packet.get_data(),
            "is_last_packet_in_block": packet.is_last_packet_in_block(),
            "block_id": block_id,
            "next_datanodes": next_datanodes
        }
        writer.write(PacketUtils.encode(json.dumps(buf).encode()))
        await writer.drain()

    async def request_new_block(self):
        reader, writer = await asyncio.open_connection(
            LOCAL_HOST, NAMENODE_PORT
        )
        message = json.dumps({"cmd": CMD_ADD_BLOCK, "inode_id": self.des_inode_id})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())
        block_id = response.get("block_id")
        blk_locs_info = response.get("blk_locs_info")

        writer.close()
        return block_id, blk_locs_info

    async def wait_for_all_ack(self):
        await self.ack_queue.join()

    async def finish(self):
        await self.data_queue.join()
        await self.ack_queue.join()
        if self.task:
            self.task.cancel()

    async def setup_pipeline(self, block_id, target, next_datanodes):
        reader, writer = await asyncio.open_connection(
            target.get("id"), target.get("port")
        )
        message = json.dumps({"cmd": CLI_DATANODE_CMD_SETUP_WRITE, "block_id": block_id, "next_datanodes": next_datanodes})
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(BUF_LEN)
        response = json.loads(data.decode())

        return reader, writer

    def close(self):
        if self.writer:
            self.writer.close()