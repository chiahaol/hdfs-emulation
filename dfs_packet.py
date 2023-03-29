from config import *

class DFSPacket:
    seqno = 0

    @classmethod
    def create_packet(cls, data, last_packet_in_block):
        self = DFSPacket(data, last_packet_in_block)
        DFSPacket.seqno += 1
        self.seqno = DFSPacket.seqno
        return self

    def __init__(self, data, last_packet_in_block):
        self.data = bytearray([])
        self.data[:] = data
        self.last_packet_in_block = last_packet_in_block

    def get_seqno(self):
        return self.seqno

    def get_data(self):
        return self.data

    def is_last_packet_in_block(self):
        return self.last_packet_in_block

    @staticmethod
    def get_chunk_header(bytes_len):
        header = []
        for i in range(4):
            header.append((bytes_len >> (i * 8)) & 0xff)
        return bytearray(header[::-1])

    @staticmethod
    def chunk_header_to_len(header):
        data_len = 0
        for i in range(4):
            data_len = data_len * 256 + header[i]
        return data_len;

    @staticmethod
    def encode(packet):
        buf = bytearray([])

        b_seqno = str(packet.get_seqno()).encode()
        b_data = packet.get_data()
        b_is_last = str(int(packet.is_last_packet_in_block())).encode()

        buf += (DFSPacket.get_chunk_header(len(b_seqno)) + b_seqno)
        buf += (DFSPacket.get_chunk_header(len(b_data)) + b_data)
        buf += (DFSPacket.get_chunk_header(len(b_is_last)) + b_is_last)

        # patch the packet length to a fixed size
        return buf + bytes(PACKET_BUF_SIZE - len(buf))


    @classmethod
    def decode(cls, encoded):
        ptr = 0
        seqno_len = DFSPacket.chunk_header_to_len(encoded[ptr: 4 + ptr])
        seqno = int(encoded[4 + ptr: 4 + ptr + seqno_len].decode())
        ptr += (4 + seqno_len)

        data_len = DFSPacket.chunk_header_to_len(encoded[ptr: 4 + ptr])
        data = encoded[4 + ptr: 4 + ptr + data_len]
        ptr += (4 + data_len)

        last_len = DFSPacket.chunk_header_to_len(encoded[ptr: 4 + ptr])
        last = bool(int(encoded[4 + ptr: 4 + ptr + last_len].decode()))

        self = DFSPacket(data, last)
        self.seqno = seqno
        return self
