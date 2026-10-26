import socket

def send_all(socket: socket.socket, bytes):
    bytes_written = 0

    while bytes_written < len(bytes):
        try:
            n = socket.send(bytes[bytes_written:])
            bytes_written += n
        except Exception as e:
            raise e

def recv_all(socket: socket.socket, length: int):
    bytes_received = 0
    data = b""

    while bytes_received < length:
        try:
            chunk = socket.recv(length - bytes_received)
            
            if not chunk:
                raise ValueError("Socket connection closed before receiving all data")
            
            data += chunk
            bytes_received += len(chunk)
        except Exception as e:
            raise e

    return data