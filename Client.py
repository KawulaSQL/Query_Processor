# Client.py
import socket
import struct

HEADER_SIZE = 4
SERVER_PORT = 5371

class Client:

    def __init__(self, sock=None):
        if sock is None:
            self.sock = socket.socket(
                            socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.sock = sock

    def connect(self, host = "localhost", port =  SERVER_PORT):
        self.sock.connect((host, port))

    def send(self, msg):
        msg_length = struct.pack('>I', len(msg))
        self.sock.sendall(msg_length + msg)
        # print(f"Sent message: {msg.decode()}")

    def receive(self) -> str :
        header = self.sock.recv(HEADER_SIZE)
        if not header:
            raise RuntimeError("Socket connection broken during header reception")
        msg_length = struct.unpack('>I', header)[0]
        chunks = []
        bytes_recd = 0
        while bytes_recd < msg_length:
            chunk = self.sock.recv(min(msg_length - bytes_recd, 2048))
            if not chunk:
                raise RuntimeError("Socket connection broken during data reception")
            chunks.append(chunk)
            bytes_recd += len(chunk)
        data = b''.join(chunks).decode()
        # print(f"Received message: {data}")
        return data