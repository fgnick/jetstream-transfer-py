import os
import socket
import redis
import time
from collections import defaultdict

# =========================
# Redis 設定
# =========================
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=False,  # ⚠️ 保留 bytes，避免隱性轉碼
)

STREAM_NAME = b"transfer_tasks"
GROUP_NAME = b"jet_engine_group"
CONSUMER_NAME = b"engine_01"

# =========================
# Consumer Group 初始化
# =========================
try:
    r.xgroup_create(
        STREAM_NAME,
        GROUP_NAME,
        id=b"$",           # ⚠️ 從建立當下開始
        mkstream=True
    )
except redis.exceptions.ResponseError:
    pass  # group 已存在


# =========================
# Socket Pool（簡化版）
# =========================
class SocketPool:
    def __init__(self):
        self._pool = defaultdict(list)

    def get(self, target_ip, port):
        key = (target_ip, port)
        if self._pool[key]:
            return self._pool[key].pop()

        sock = socket.create_connection((target_ip, port), timeout=5)
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
# Zero-copy 傳輸核心
# =========================
def send_file_zero_copy(sock, file_path):
    with open(file_path, "rb") as f:
        fd = f.fileno()
        filesize = os.fstat(fd).st_size

        offset = 0
        while offset < filesize:
            sent = os.sendfile(
                sock.fileno(),
                fd,
                offset,
                filesize - offset
            )
            if sent == 0:
                raise RuntimeError("Connection closed during sendfile")
            offset += sent


# =========================
# Jet Engine 主迴圈
# =========================
def start_jet_engine():
    print(f"🚀 {CONSUMER_NAME.decode()} 噴射引擎啟動")

    while True:
        try:
            # 一次拿多筆，減少 Redis round-trip
            tasks = r.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                {STREAM_NAME: b">"},
                count=10,
                block=2000
            )

            if not tasks:
                continue

            for _, messages in tasks:
                for msg_id, data in messages:
                    file_path = data[b"file_path"].decode()
                    target_ip = data[b"target_ip"].decode()
                    port = int(data.get(b"port", b"9000"))

                    sock = None
                    try:
                        sock = socket_pool.get(target_ip, port)

                        send_file_zero_copy(sock, file_path)

                        # 傳輸成功 → ACK
                        r.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        print(f"✅ 噴射完成 {file_path} ({msg_id.decode()})")

                        socket_pool.put(target_ip, port, sock)

                    except Exception as e:
                        print(f"❌ 噴射失敗 {file_path}: {e}")

                        if sock:
                            socket_pool.discard(sock)
                        # 不 ACK，留給 XPENDING / XCLAIM

        except Exception as loop_error:
            print(f"⚠️ 主迴圈錯誤: {loop_error}")
            time.sleep(1)


if __name__ == "__main__":
    start_jet_engine()
