import socket
import struct
import os

def start_receiver(host='0.0.0.0', port=9000, save_dir='./received_files'):
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    
    print(f"🎧 Receiver listening on {host}:{port}")

    while True:
        client, addr = server.accept()
        print(f"⭐ Connection from {addr}")

        try:
            while True:
                # 1. 讀取 Header (8 bytes)
                header = recv_all(client, 8)
                if not header:
                    break  # Client closed connection properly

                # 解析檔案大小
                filesize = struct.unpack("!Q", header)[0]
                print(f"📦 Incoming file size: {filesize} bytes")

                # 2. 讀取檔案內容
                # 這裡為了簡單測試，直接寫死檔名，實務上 Protocol 應該還要包含檔名長度與檔名
                filename = os.path.join(save_dir, f"received_{int(os.times().elapsed)}_{filesize}.bin")
                
                start_recv_file(client, filename, filesize)
                print(f"✅ Saved to {filename}")

        except Exception as e:
            print(f"⚠️ Connection error: {e}")
        finally:
            client.close()
            print("🔌 Connection closed")

# Helper to ensure we get exactly n bytes
def recv_all(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

# Receive file content
def start_recv_file(sock, filename, filesize):
    received = 0
    with open(filename, "wb") as f:
        while received < filesize:
            # 每次讀取一小塊 (例如 64KB)
            chunk_size = min(65536, filesize - received)
            data = sock.recv(chunk_size)
            if not data:
                raise RuntimeError("Connection closed inside file body")
            f.write(data)
            received += len(data)
    # [Fix] Send ACK byte to prevent sender deadlock
    sock.sendall(b'\x01')

if __name__ == "__main__":
    start_receiver()
