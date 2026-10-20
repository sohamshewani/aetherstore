# aetherstore
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/aetherstore/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/aetherstore/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/aetherstore) [![Stars](https://img.shields.io/github/stars/sohamshewani/aetherstore?style=flat)](https://github.com/sohamshewani/aetherstore)

## Executive Summary & Problem Space

`aetherstore` is an enterprise-grade, high-throughput embedded key-value storage engine engineered in modern C (C11). Designed for latency-critical distributed systems, high-frequency trading platforms, and localized caching layers, `aetherstore` solves the foundational bottlenecks of traditional storage engines: lock contention under high core counts, unmitigated disk I/O serialization, and unpredictable garbage collection pauses.

Traditional storage implementations often suffer from global mutex bottlenecks across read/write paths and inefficient memory mapping that results in thrashing under memory pressure. `aetherstore` addresses these challenges by implementing:
- A lock-free/lock-efficient Reader-Writer Latch (`rwlatch.c`) architecture tailored for NUMA-aware multi-core processors.
- A deterministic, pinned Buffer Pool (`buffer_pool.c`) executing O(1) page eviction and localized frame locking.
- A Write-Ahead Log (`wal.c`) subsystem utilizing direct asynchronous, buffered flushes with vectorized group-commit patterns to maximize disk bandwidth saturation.
- An in-memory and on-disk B-Tree (`btree.c`) structure guaranteeing strict structural invariants and predictable traversal latencies.

---

## Structural ASCII Architecture / Dataflow Diagram

The system is architected in distinct decoupling layers, separating thread-safe client interfaces from storage and disk persistence engines.

```
 +-----------------------------------------------------------------------+
 |                     Client Application / Workload                     |
 +-----------------------------------------------------------------------+
                                    |
                                    v
 +-----------------------------------------------------------------------+
 |                       Engine API Layer (engine.h)                     |
 |  - Transaction Management    - Point Lookups    - Range Scans         |
 +-----------------------------------------------------------------------+
            |                                         |
            v                                         v
 +---------------------+                   +-----------------------------+
 |   Concurrency &     |                   |       Core Engine Subsystems|
 |  RWLatch (rwlatch.c)|                   +-----------------------------+
 |  - Shared/Exclusive |                   | B-Tree Index (btree.c)      |
 |    Latch Primitives |                   | - Node Splits / Merges      |
 +---------------------+                   | - Key-Value Mappings        |
            |                              +-----------------------------+
            |                                         |
            +--------------------+--------------------+
                                 |
                                 v
 +-----------------------------------------------------------------------+
 |                    Buffer Pool Manager (buffer_pool.c)                |
 |  - Pinned Frames    - LRU/Clock Eviction    - Frame-level Latches     |
 +-----------------------------------------------------------------------+
            |                                         |
            v                                         v
 +---------------------+                   +-----------------------------+
 |  Write-Ahead Log    |                   |       OS / File System      |
 |    (wal.c)          |                   +-----------------------------+
 |  - fsync() Barriers |                   | - Direct I/O Block Devices  |
 |  - Sequential Log   |                   | - Page Cache Integration    |
 +---------------------+                   +-----------------------------+
            |                                         |
            v                                         v
 +-----------------------------------------------------------------------+
 |                         Non-Volatile Storage                          |
 +-----------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

`aetherstore` implements a fine-grained concurrency model designed to eliminate global synchronization points across concurrent workloads.

- **Reader-Writer Latches (`rwlatch.c`):** Concurrency across B-Tree nodes and buffer pool frames is managed using optimized reader-writer latches. Writers acquire exclusive access (`W_LOCK`) to mutate structural tree nodes or dirty buffer frames, while readers acquire shared access (`R_LOCK`) concurrently. Starvation prevention mechanisms ensure bounded wait times for high-priority write operations.
- **Memory Ownership & Zero-Copy Paths:** Memory blocks allocated via `include/aether/types.h` enforce strict ownership lifecycles. Buffers routed through the buffer pool remain pinned during active query execution, preventing asynchronous eviction by background threads. 
- **Atomic Operations:** Thread-safe state transitions, reference counting, and sequence numbering leverage C11 atomic primitives (`stdatomic.h`) with sequential consistency (`memory_order_seq_cst`) and acquire-release semantics to guarantee hardware-level thread safety without kernel trap overhead.

---

## Performance Characteristics & Algorithmic Complexity Table

The internal algorithms of `aetherstore` are engineered to provide bounded worst-case performance guarantees suitable for strict SLA environments.

| Operation | Average Time Complexity | Worst-Case Complexity | Estimated Latency (P99) | Bottleneck Vector |
| :--- | :--- | :--- | :--- | :--- |
| **Point Read (Get)** | $O(\log_B N)$ | $O(\log_B N)$ | $< 12 \mu s$ | Buffer pool frame miss / Disk Seek |
| **Point Write (Put)** | $O(\log_B N + \text{WAL})$ | $O(B + \text{WAL})$ | $< 35 \mu s$ | Synchronous WAL disk flush (`fsync`) |
| **Range Scan (Iter)** | $O(\log_B N + K)$ | $O(\log_B N + K)$ | $< 50 \mu s$ | Leaf node traversal / Memory bandwidth |
| **Node Split / Merge**| $O(B)$ | $O(B)$ | $< 120 \mu s$ | Exclusive structural latch contention |
| **Buffer Eviction**   | $O(1)$ | $O(N_{\text{pool}})$ | $< 5 \mu s$ | LRU list traversal under high pressure |

*Note: $N$ represents total keys, $B$ represents B-Tree branching factor/node size, and $K$ represents keys retrieved in a range.*

---

## Security Architecture & Threat Model

`aetherstore` treats security as a core architectural pillar, adopting defensive programming models to protect against memory corruption and data injection vectors.

- **Input Boundaries & Sanitization:** All incoming keys and payloads pass through strict length and schema boundaries defined in `include/aether/types.h`. Unbounded string inputs and raw pointers are prohibited; explicit sizing parameters are enforced globally.
- **Memory Corruption Protections:** The codebase is compiled with hardened compiler flags (`-D_FORTIFY_SOURCE=2`, stack protectors, and position-independent executables). Strict bounds checking is applied on all buffer pool frame manipulations and B-Tree node layouts to prevent buffer over-reads and writes.
- **Sanitizer Integration:** The build infrastructure includes native support for AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and ThreadSanitizer (TSan), executed continuously via automated CI pipelines.

---

## Build, Verification & Benchmarking Recipes

To compile the repository, execute the test suite, and run performance benchmarks, use the following production recipes.

### Prerequisites
- GCC (11+) or Clang (13+) supporting C11
- GNU Make
- POSIX-compliant environment (Linux / macOS)

### 1. Build the Engine and Test Suite
```bash
# Compile libraries, test binaries, and benchmark utilities
make clean
make all
```

### 2. Execute Invariant & Unit Tests
Verify correctness, memory safety, and structural invariants:
```bash
make test
# Or execute the test binary directly:
./tests/test_invariants
```

### 3. Run Performance Benchmarks
Profile engine throughput and latency distributions:
```bash
make bench
# Or execute the benchmark binary directly:
./benchmarks/bench_engine