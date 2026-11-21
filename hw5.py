"""
Where solution code to HW5 should be written.  No other files should
be modified.
"""

from multiprocessing.dummy import shutdown
import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging

#Send: I split the file into numbered packets and 
#send up to two at a time. 
#I wait for acknowledgments, resend lost packets, and adjust the timeout based on response times. 
#I send FIN when all the data is acknowledged.
# Receive: I receive packets, write the data in order, and avoid duplicates. 
#I send acknowledgments to confirm delivery and handle FIN to finish the transfer.

def send(sock: socket.socket, data: bytes):
    """
    Implementation of the sending logic for sending data over a slow,
    lossy, constrained network.

    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.
        data -- A bytes object, containing the data to send over the network.
    """

    # Naive implementation where we chunk the data to be sent into
    # packets as large as the network will allow, and then send them
    # over the network, pausing half a second between sends to let the
    # network "rest" :)
    # HEADER = struct.Struct("!I") # 4-byte sequence
    packet_header = struct.Struct("!cQ")
    block_size = homework5.MAX_PACKET - packet_header.size
    waiting = {}
    position = 0
    average = None
    rto = 0.5
    saved_timeout = sock.gettimeout()
    variation = 0.0
    try:
        while position < len(data) or waiting:
            while len(waiting) <2 and position <len(data):
                block = data[position:position + block_size]
                message = packet_header.pack(b"D", position) + block
                sock.send(message)
                now = time.monotonic()
                waiting[position] = {
                    "message":message, "sent":now, "deadline": now + rto, "interval":rto, "repeated":False,}
                position += len(block)
            next_retry = min(item["deadline"] for item in waiting.values())
            sock.settimeout(max(0.001, next_retry - time.monotonic()))
            try:
                response = sock.recv(homework5.MAX_PACKET)
            except socket.timeout:
                response = b""
            now = time.monotonic()
            if len(response) == packet_header.size:
                kind, number = packet_header.unpack(response)
                if kind == b"A" and number in waiting:
                    item = waiting.pop(number)
                    if not item["repeated"]:
                        sample = now - item["sent"]
                        if average is None:
                            average = sample
                            variation = sample / 2
                        else:
                            variation = 0.75 * variation + 0.25 * abs(sample - average)
                            average = 0.875 * average + 0.125 * sample
                        rto = max(0.01, average + 4 * variation)
            for item in waiting.values():
                if now >= item["deadline"]:
                    sock.send(item["message"])
                    item["interval"] *= 2
                    item["deadline"] = now + item["interval"]
                    item["repeated"] = True
        finish = packet_header.pack(b"F", len(data)) + struct.pack("!d", rto)
        interval = rto
        while True:
            sock.send(finish)
            deadline = time.monotonic() + interval
            while time.monotonic() < deadline:
                sock.settimeout(max(0.001, deadline - time.monotonic()))
                try:
                    response = sock.recv(homework5.MAX_PACKET)
                except socket.timeout:
                    break
                if response == packet_header.pack(b"K", len(data)):
                    sock.send(packet_header.pack(b"C", len(data)))
                    return
            interval *= 2
    finally:
        sock.settimeout(saved_timeout)
def recv(sock: socket.socket, dest: io.BufferedIOBase) -> int:
    """
    Implementation of the receiving logic for receiving data over a slow,
    lossy, constrained network.
    Args:
        sock -- A socket object, constructed and initialized to communicate
                over a simulated lossy network.

    Return:
        The number of bytes written to the destination.
    """
    packet_header = struct.Struct("!cQ")
    next_byte = 0
    blocks = {}
    closing = False
    close_wait = 1.0
    saved_timeout = sock.gettimeout()
    try:
        sock.settimeout(None)
        while True:
            try:
                message = sock.recv(homework5.MAX_PACKET)
            except socket.timeout:
                if closing:
                    return next_byte
                continue
            if len(message) < packet_header.size:
                continue
            kind, number = packet_header.unpack(message[:packet_header.size])
            content = message[packet_header.size:]
            if kind == b"D" and not closing:
                if number >= next_byte:
                    blocks.setdefault(number, content)
                while next_byte in blocks:
                    block = blocks.pop(next_byte)
                    dest.write(block)
                    next_byte += len(block)
                sock.send(packet_header.pack(b"A", number))
            elif kind == b"F" and number == next_byte and len(content) == 8:
                if not closing:
                    dest.flush()
                    closing = True
                    close_wait = max(2.0, 32 * struct.unpack("!d", content)[0])
                sock.send(packet_header.pack(b"K", next_byte))
                sock.settimeout(close_wait)
                close_wait *= 2
            elif kind == b"C" and closing and number == next_byte:
                return next_byte
    finally:
        sock.settimeout(saved_timeout)