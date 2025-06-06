import socket  # noqa: F401
from app.api_versions import ApiVersionsResponse
from app.request import KafkaRequestHeader
from app.response import KafkaResponse, KafkaResponseHeader


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept()  # wait for client

    message_size = conn.recv(4)
    message_size = int.from_bytes(message_size, "big", signed=True)

    message = conn.recv(message_size)
    request_header = KafkaRequestHeader(message)

    response_header = KafkaResponseHeader(request_header.correlation_id)
    response_body = None
    if request_header.api_key == 18:
        response_body = ApiVersionsResponse()
    response = KafkaResponse(response_header, response_body)
    conn.sendall(bytes(response.size()) + bytes(response))


if __name__ == "__main__":
    main()
