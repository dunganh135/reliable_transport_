import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3


def make_ack(seq_num):
    header = PacketHeader(type=ACK, seq_num=seq_num, length=0)
    header.checksum = 0
    header.checksum = compute_checksum(header / b"")
    return bytes(header / b"")


def parse_packet(raw):
    if len(raw) < 16:
        return None
    try:
        header = PacketHeader(raw[:16])
        payload = raw[16 : 16 + header.length]
        if len(payload) != header.length:
            return None
        received = header.checksum
        header.checksum = 0
        if compute_checksum(header / payload) != received:
            return None
        return header.type, header.seq_num, payload
    except Exception:
        return None


def receiver(receiver_ip, receiver_port, window_size):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((receiver_ip, receiver_port))
    out = sys.stdout.buffer

    connected = False
    expected = 1
    buffer = {}
    window_size = max(1, window_size)

    while True:
        raw, address = sock.recvfrom(2048)
        parsed = parse_packet(raw)
        if parsed is None:
            continue
        pkt_type, seq_num, payload = parsed

        if pkt_type == START:
            if not connected:
                connected = True
                expected = 1
                buffer = {}
                sock.sendto(make_ack(seq_num + 1), address)
            continue

        if not connected:
            continue

        if pkt_type == DATA:
            if seq_num >= expected + window_size:
                continue
            if seq_num >= expected and seq_num not in buffer:
                buffer[seq_num] = payload
            while expected in buffer:
                out.write(buffer.pop(expected))
                expected += 1
            out.flush()
            sock.sendto(make_ack(seq_num), address)

        elif pkt_type == END:
            sock.sendto(make_ack(seq_num + 1), address)
            out.flush()
            break

    sock.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()