import socket  # noqa: F401
from app.response import ResponseHeader


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept()  # wait for client
    header = ResponseHeader(7)
    conn.sendall(header.size())
    conn.sendall(bytes(header))


if __name__ == "__main__":
    main()
