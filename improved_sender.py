import os
import socket
import redis
import time
import struct
from collections import defaultdict

# =========================
# Redis 設定
# =========================
# 注意: 如果沒有 Redis Server，這行會報錯。
try:
    r = redis.Redis(
        host="localhost",
        port=6379,
        decode_responses=False,
    )
    REDIS_AVAILABLE = True
except Exception:
    REDIS_AVAILABLE = False
    print("Redis not connected (Mock mode)")

STREAM_NAME = b"transfer_tasks"
GROUP_NAME = b"jet_engine_group"
CONSUMER_NAME = b"engine_01"

# =========================
# Socket Pool
# =========================
class SocketPool:
    def __init__(self):
        self._pool = defaultdict(list)

    def get(self, target_ip, port):
        key = (target_ip, port)
        if self._pool[key]:
            # 取出一個既有的 socket，但要確認它還是活的
            sock = self._pool[key].pop()
            return sock

        # 建立新連線
        print(f"Creating new connection to {target_ip}:{port}")
        sock = socket.create_connection((target_ip, port), timeout=5)
        
        # TCP_NODELAY: 關閉 Nagle 演算法，對即時傳輸很重要
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(30)
        return sock

    def put(self, target_ip, port, sock):
        self._pool[(target_ip, port)].append(sock)

    def discard(self, sock):
        try:
            sock.close()
        except Exception:
            pass

socket_pool = SocketPool()

# =========================
# 核心傳輸: Cross-Platform Zero-Copy + Protocol --> Windows 就可用了
# =========================
def send_file_with_protocol(sock, file_path):
    # 使用跨平台 sendfile 並加上長度 Header 防止黏包。
    # Protocol: [8 bytes Filesize (Big-Endian)] + [File Body]
    
    with open(file_path, "rb") as f:
        # 1. 取得檔案大小
        filesize = os.fstat(f.fileno()).st_size
        
        # 2. 發送 Header (8 bytes)
        # !Q = Big-Endian Unsigned Long Long (8 bytes)
        header = struct.pack("!Q", filesize)
        sock.sendall(header)

        # 3. 發送檔案本體 (Zero-Copy)
        # Windows: 自動使用 TransmitFile
        # Linux: 自動使用 os.sendfile
        print(f"Sending {filesize} bytes...")
        sent = sock.sendfile(f, offset=0, count=filesize)
        
        if sent == 0:
            raise RuntimeError("Connection closed during sendfile")
        
        # 4. 可選: 等待接收端簡單 ACK (避免太快發送下一個把 buffer 塞爆)
        sock.recv(1) 

# =========================
# Jet Engine 主迴圈
# =========================
def start_jet_engine():
    # 初始化 Group (如果是第一次執行)
    if REDIS_AVAILABLE:
        try:
            r.xgroup_create(STREAM_NAME, GROUP_NAME, id=b"$", mkstream=True)
        except redis.exceptions.ResponseError:
            pass
    
    print(f"🚀 {CONSUMER_NAME.decode()} 噴射引擎啟動")

    while True:
        try:
            if not REDIS_AVAILABLE:
                time.sleep(5)
                continue

            # 讀取任務
            tasks = r.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: b">"},
                count=1,
                block=2000
            )

            if not tasks:
                continue

            for _, messages in tasks:
                for msg_id, data in messages:
                    file_path = data.get(b"file_path", b"").decode()
                    target_ip = data.get(b"target_ip", b"127.0.0.1").decode()
                    port = int(data.get(b"port", b"9000"))

                    if not file_path or not os.path.exists(file_path):
                        print(f"⚠️ File not found: {file_path}")
                        r.xack(STREAM_NAME, GROUP_NAME, msg_id) # 避免卡死
                        continue

                    sock = None
                    try:
                        sock = socket_pool.get(target_ip, port)

                        send_file_with_protocol(sock, file_path)

                        # 傳輸成功
                        print(f"✅ 完成: {file_path}")
                        r.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        
                        # 歸還 socket
                        socket_pool.put(target_ip, port, sock)

                    except Exception as e:
                        print(f"❌ 失敗 {file_path}: {e}")
                        if sock:
                            socket_pool.discard(sock)

        except Exception as loop_error:
            print(f"⚠️ Loop Error: {loop_error}")
            time.sleep(1)

if __name__ == "__main__":
    start_jet_engine()
