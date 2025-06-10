import socket  # noqa: F401
import threading
from app.api_versions import ApiVersionsRequest, ApiVersionsResponse
from app.request import KafkaRequest, KafkaRequestBody, KafkaRequestHeader
from app.response import KafkaResponse, KafkaResponseHeader


def handle_connection(conn, addr):
    print(f"[+] Connected by {addr}")
    try:
        while True:
            message_size = conn.recv(4)
            message_size = int.from_bytes(message_size, "big", signed=True)

            message = conn.recv(message_size)
            it = iter(message)
            request_header = KafkaRequestHeader.deserialize(it)
            match request_header.api_key:
                case 18:
                    request_body = ApiVersionsRequest.deserialize(it)
                case _:
                    request_body = KafkaRequestBody()
            request = KafkaRequest(request_header, request_body)

            response_header = KafkaResponseHeader(request_header.correlation_id)
            response_body = None
            if request_header.api_key == 18:
                response_body = ApiVersionsResponse(request_header.api_version)
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
        conn, addr = server.accept()  # wait for client
        thread = threading.Thread(target=handle_connection, args=(conn, addr))
        thread.start()


if __name__ == "__main__":
    main()
