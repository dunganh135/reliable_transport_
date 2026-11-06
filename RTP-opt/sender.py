import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START = 0
END = 1
DATA = 2
ACK = 3
MAX_DATA = 1456
TIMEOUT = 0.5


def make_packet(pkt_type, seq_num, payload=b""):
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=len(payload))
    header.checksum = 0
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def parse_ack(raw):
    if len(raw) < 16:
        return None
    try:
        header = PacketHeader(raw[:16])
        payload = raw[16 : 16 + header.length]
        received = header.checksum
        header.checksum = 0
        if compute_checksum(header / payload) != received:
            return None
        if header.type != ACK:
            return None
        return header.seq_num
    except Exception:
        return None


def wait_for_ack(sock, deadline):
    remaining = deadline - time.time()
    if remaining <= 0:
        return "timeout"
    sock.settimeout(remaining)
    try:
        raw, _ = sock.recvfrom(2048)
    except socket.timeout:
        return "timeout"
    except OSError:
        return None
    return parse_ack(raw)


def sender(receiver_ip, receiver_port, window_size):
    data = sys.stdin.buffer.read()
    chunks = [data[i : i + MAX_DATA] for i in range(0, len(data), MAX_DATA)]
    total = len(chunks)
    dest = (receiver_ip, receiver_port)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    start_pkt = make_packet(START, 0)
    while True:
        sock.sendto(start_pkt, dest)
        deadline = time.time() + TIMEOUT
        connected = False
        while True:
            result = wait_for_ack(sock, deadline)
            if result == "timeout":
                break
            if result is not None and result >= 1:
                connected = True
                break
        if connected:
            break

    packets = {}
    for i, chunk in enumerate(chunks):
        packets[i + 1] = make_packet(DATA, i + 1, chunk)

    base = 1
    next_seq = 1
    last_seq = total
    window_size = max(1, window_size)
    acked = set()
    deadline = time.time() + TIMEOUT

    while base <= last_seq:
        while next_seq < base + window_size and next_seq <= last_seq:
            sock.sendto(packets[next_seq], dest)
            next_seq += 1

        result = wait_for_ack(sock, deadline)
        if result == "timeout":
            for seq in range(base, next_seq):
                if seq not in acked:
                    sock.sendto(packets[seq], dest)
            deadline = time.time() + TIMEOUT
        elif result is not None and base <= result <= last_seq:
            acked.add(result)
            if result == base:
                while base in acked:
                    acked.discard(base)
                    base += 1
                deadline = time.time() + TIMEOUT

    end_seq = last_seq + 1
    end_pkt = make_packet(END, end_seq)
    sock.sendto(end_pkt, dest)
    end_deadline = time.time() + TIMEOUT
    while True:
        result = wait_for_ack(sock, end_deadline)
        if result == "timeout":
            break
        if result is not None and result >= end_seq:
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

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()