import socket  # noqa: F401
import threading
from app.api_versions import ApiVersionsRequest, ApiVersionsResponse
from app.describe_topic_partitions import DescribeTopicPartitionsRequest
from app.request import KafkaRequestBody, KafkaRequestHeader
from app.response import KafkaResponse, KafkaResponseHeader

API_ROUTE = {
    18: ApiVersionsRequest,  # ApiVersionsRequest
    75: DescribeTopicPartitionsRequest,  # DescribeTopicPartitionsRequest
}


def handle_connection(conn: socket.socket, addr: tuple[str, int]) -> None:
    print(f"[+] Connected by {addr}")
    try:
        while True:
            # Read exactly 4 bytes for the message size
            message_size_bytes = b""
            while len(message_size_bytes) < 4:
                chunk = conn.recv(4 - len(message_size_bytes))
                if not chunk:
                    raise ConnectionError(
                        "Connection closed while reading message size"
                    )
                message_size_bytes += chunk
            message_size = int.from_bytes(message_size_bytes, "big", signed=True)

            # Read the full message
            message = b""
            while len(message) < message_size:
                chunk = conn.recv(message_size - len(message))
                if not chunk:
                    raise ConnectionError(
                        "Connection closed while reading message body"
                    )
                message += chunk

            it = iter(message)
            request_header = KafkaRequestHeader.deserialize(it)
            request_body = API_ROUTE.get(
                request_header.api_key, KafkaRequestBody
            ).deserialize(it)

            response_header = KafkaResponseHeader(request_header.correlation_id)
            response_body = (
                ApiVersionsResponse(request_header.api_version)
                if request_header.api_key == 18
                else None
            )
            response = KafkaResponse(response_header, response_body)
            conn.sendall(bytes(response.size()) + bytes(response))
    except Exception as e:
        print(f"[!] Error with {addr}: {e}")
    finally:
        conn.close()
        print(f"[-] Disconnected {addr}")


def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("[*] server listening on localhost:9092")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_connection, args=(conn, addr))
        thread.start()


if __name__ == "__main__":
    main()
