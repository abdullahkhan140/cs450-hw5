"""
Where solution code to HW5 should be written.  No other files should
be modified.
"""

import socket
import io
import time
import typing
import struct
import homework5
import homework5.logging


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
    block_size = homework5.MAX_PACKET - packet_handler.size
    waiting = {}
    pos = 0
    average = None
    rto = 0.5
    saved_timeout = sock.gettimeout()
    var = 0.0
    try:
        while pos < len(data) or waiting:
            while len(waiting) <2 and pos <len(data):
                block = data[pos:pos + block_size]
                message = packet_handler.pack(b"D", pos) + block
                sock.send(message)
                now = time.montonic()
                waiting[pos] = {
                    "message":message, "sent":sent, "deadline": now + rto, "interval":rto, "repeated":False,}
                position += len(block)
            next_entry = min(item["deadline"] for item in waiting.values())
            

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
    logger = homework5.logging.get_logger("hw5-receiver")
    # Naive solution, where we continually read data off the socket
    # until we don't receive any more data, and then return.
    num_bytes = 0
    while True:
        data = sock.recv(homework5.MAX_PACKET)
        if not data:
            break
        logger.info("Received %d bytes", len(data))
        dest.write(data)
        num_bytes += len(data)
        dest.flush()
    return num_bytes
