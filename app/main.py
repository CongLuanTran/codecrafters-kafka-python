import socket  # noqa: F401
from app.request import RequestHeader
from app.response import ResponseHeader


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept()  # wait for client

    message_size = conn.recv(4)
    message_size = int.from_bytes(message_size, "big", signed=True)

    message = conn.recv(message_size)
    request_header = RequestHeader(message)

    response_header = ResponseHeader(request_header.correlation_id)
    conn.sendall(response_header.size())
    conn.sendall(bytes(response_header))


if __name__ == "__main__":
    main()
