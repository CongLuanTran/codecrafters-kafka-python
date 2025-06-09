import socket  # noqa: F401
from api_versions import ApiVersionsResponse
from request import KafkaRequest
from response import KafkaResponse, KafkaResponseHeader


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept()  # wait for client
    while True:
        message_size = conn.recv(4)
        message_size = int.from_bytes(message_size, "big", signed=True)

        message = conn.recv(message_size)
        request = KafkaRequest.deserialize(iter(message))

        response_header = KafkaResponseHeader(request.header.correlation_id)
        response_body = None
        if request.header.api_key == 18:
            response_body = ApiVersionsResponse(request.header.api_version)
        response = KafkaResponse(response_header, response_body)
        conn.sendall(bytes(response.size()) + bytes(response))


if __name__ == "__main__":
    main()
