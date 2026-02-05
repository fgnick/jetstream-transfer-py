import threading
import time
import socket
import os
import sys
import struct

# Create a dummy file for testing
TEST_FILENAME = "test_payload_1mb.bin"
with open(TEST_FILENAME, "wb") as f:
    f.write(os.urandom(1024 * 1024)) # 1MB

# Import the user's modules
try:
    import improved_sender
    import simple_receiver
except ImportError:
    # Ensure current dir is in path
    sys.path.append(os.getcwd())
    import improved_sender
    import simple_receiver

def test_receiver_thread():
    try:
        # Run receiver on a non-standard port to avoid conflicts
        simple_receiver.start_receiver(port=9991, save_dir='./test_output')
    except Exception as e:
        print(f"[Receiver Error] {e}")

def test_sender():
    time.sleep(1) # Give receiver time to start
    print("--- Starting Sender Test ---")
    
    # Manually create a socket like SocketPool does
    sock = socket.create_connection(('127.0.0.1', 9991), timeout=5)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    # Set a short timeout to detect deadlock quickly
    sock.settimeout(3.0) 
    
    try:
        print(f"Sending {TEST_FILENAME}...")
        improved_sender.send_file_with_protocol(sock, TEST_FILENAME)
        print("Success! File sent.")
    except socket.timeout:
        print("❌ FAILED: Socket timed out! (Likely deadlock at sock.recv(1))")
    except Exception as e:
        print(f"❌ FAILED: {e}")
    finally:
        sock.close()

if __name__ == "__main__":
    # Start receiver in background
    t = threading.Thread(target=test_receiver_thread, daemon=True)
    t.start()
    
    # Run sender logic
    test_sender()
