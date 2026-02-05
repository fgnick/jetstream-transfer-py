# Zero-Copy Jetstream Transfer Engine

A high-throughput, zero-copy file transfer engine built on Linux `sendfile`, designed to stream files directly from disk to network with minimal CPU overhead.

This repository serves as a reference implementation of a data-plane–oriented transfer pipeline using kernel-level I/O and Redis Streams for task coordination.

---

## Overview

This engine consumes file transfer tasks from a Redis Stream and dispatches them to remote receivers using Linux zero-copy I/O.

Instead of reading file contents into userspace buffers, files are transferred directly from the kernel page cache to the network stack via `sendfile`, significantly reducing memory copies and CPU usage.

The design emphasizes throughput, simplicity, and reliability over payload inspection or transformation.

---

## Architecture

Disk / Page Cache
|
| (sendfile)
v
Kernel Network Stack
|
v
NIC ---> Remote Receiver

Task coordination is handled via Redis Streams:
Producer → Redis Stream → Consumer Group → Transfer Engine

---

## Key Features

- True zero-copy file transfer
- No userspace buffering of file contents
- Redis Streams consumer groups for reliable task dispatch
- Connection pooling to reduce TCP handshake overhead
- Backpressure-friendly blocking I/O model
- At-least-once delivery semantics (ACK on successful transfer)

---

## Requirements

- Linux (kernel with `sendfile` support)
- Python 3.8 or later
- Redis 6.0 or later
- Local or LAN-based network environment

> TLS encryption is intentionally not supported, as it disables zero-copy behavior.

---

## Running the Engine

1. Start Redis
2. Ensure the destination receiver is listening on the target port
3. Run the transfer engine:
   python zero-copy-dispatch-data.py

The engine will continuously consume tasks and dispatch files until stopped.

---

## Disclaimer

This project is provided as a technical demonstration and reference implementation.
It is not intended to be production-ready and comes with no guarantees regarding stability, security, or long-term support.




