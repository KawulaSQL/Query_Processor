#Server.py
import socket
import threading
import struct

from QueryProcessor import QueryProcessor
from utils.result import ExecutionResult, get_execution_result

SERVER_PORT = 5371
BASE_PATH = "./Storage_Manager/storage"
# BASE_PATH = "./db-test"
HEADER_SIZE = 4

class Server:
    def __init__(self):
        self.clients = {}
        self.host = "localhost"
        self.port = SERVER_PORT
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.query_processor = QueryProcessor(BASE_PATH)

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        print(">>> KawulaSQL Server is running at localhost:" + str(SERVER_PORT))

        while True:
            client_socket, address = self.server_socket.accept()
            client_id = len(self.clients) + 1
            self.clients[client_id] = client_socket
            print(f"New client connected: Client {client_id} at {address}")
            
            thread = threading.Thread(target=self.serve_client, args=(client_id, client_socket))
            thread.start()

    def send(self, conn, msg):
        msg_length = struct.pack('>I', len(msg))
        conn.sendall(msg_length + msg)

    def receive(self, conn) -> str:
        header = conn.recv(HEADER_SIZE)
        if not header:
            raise RuntimeError("Socket connection broken during header reception")
        msg_length = struct.unpack('>I', header)[0]
        chunks = []
        bytes_recd = 0
        while bytes_recd < msg_length:
            chunk = conn.recv(min(msg_length - bytes_recd, 2048))  # Receive in chunks
            if not chunk:
                raise RuntimeError("Socket connection broken during data reception")
            chunks.append(chunk)
            bytes_recd += len(chunk)
        return b''.join(chunks).decode()


    def serve_client(self, client_id, client_socket):
        try:
            while True:
                query_request = self.receive(client_socket)
                try:
                    result = self.query_processor.process_query(query_request)
                    if isinstance(result, ExecutionResult):
                        try:
                            response = get_execution_result(result)
                            self.send(client_socket, response.encode())
                        except Exception as e:
                            print(f"Error while sending to client: {str(e)}")
                    else:
                        print("Unexpected result type.")
                except Exception as e:
                    print(f"Error processing query: {str(e)}")
        except ConnectionResetError:
            print(f"Client {client_id} disconnected")
        except Exception as e:
            print(f"Error while receiving from client: {str(e)}")
        finally:
            client_socket.close()
            del self.clients[client_id]

if __name__ == "__main__":
    server = Server()
    server.start()
