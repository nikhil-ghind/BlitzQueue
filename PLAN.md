# Blitz Queue

## Project Overview
Low-latency message queue in C++17 using Boost.Asio for async I/O and gRPC for service communication. Core data structure is a lock-free MPMC (multi-producer, multi-consumer) queue achieving 2M+ messages/second throughput with sub-microsecond latency. Designed for financial and real-time systems requiring deterministic performance.

## Tech Stack
- **Language:** C++17
- **Async I/O:** Boost.Asio 1.82+
- **RPC:** gRPC 1.60+, Protobuf 3
- **Build:** CMake 3.20+, Conan or vcpkg for dependencies
- **Testing:** Google Test, Google Benchmark
- **Profiling:** perf, FlameGraph, Intel VTune (optional)

## Architecture Overview
```
┌────────────┐  gRPC   ┌──────────────┐         ┌────────────┐
│ Producer   │ ──────► │  MQ Server   │ ──────► │ Consumer   │
│ Client(s)  │         │  (Boost.Asio)│         │ Client(s)  │
└────────────┘         └──────┬───────┘         └────────────┘
                              │
                    ┌─────────┴─────────┐
                    │  Lock-Free MPMC   │
                    │  Ring Buffer      │
                    └───────────────────┘
```

- **MPMC Ring Buffer:** Fixed-size, cache-line-padded, CAS-based lock-free queue
- **MQ Server:** Boost.Asio event loop, accepts gRPC connections, routes messages from producers to topic queues, dispatches to consumers
- **Producer Client:** gRPC stub, batches messages, async send
- **Consumer Client:** gRPC streaming, pull-based consumption with acknowledgments
- **Topic Manager:** Named queues with configurable retention and fan-out

## Phase 1: Lock-Free MPMC Ring Buffer
**Goal:** Implement the core lock-free queue data structure.

### Tasks
1. Project structure:
   ```
   cppLowLatencyMessageQueue/
   ├── CMakeLists.txt
   ├── include/
   │   ├── mpmc_queue.h         # Lock-free MPMC queue
   │   ├── message.h            # Message struct
   │   ├── topic.h              # Topic (named queue)
   │   ├── server.h             # MQ Server
   │   ├── producer_client.h
   │   └── consumer_client.h
   ├── src/
   │   ├── mpmc_queue.cpp
   │   ├── topic.cpp
   │   ├── server.cpp
   │   ├── producer_client.cpp
   │   └── consumer_client.cpp
   ├── proto/
   │   └── mq.proto
   ├── tests/
   │   ├── test_mpmc_queue.cpp
   │   └── test_server.cpp
   ├── benchmarks/
   │   ├── bench_queue.cpp
   │   └── bench_throughput.cpp
   └── tools/
       ├── producer_cli.cpp
       └── consumer_cli.cpp
   ```
2. `include/mpmc_queue.h`:
   ```cpp
   template<typename T>
   class MPMCQueue {
   public:
       explicit MPMCQueue(size_t capacity);  // must be power of 2
       bool try_push(const T& item);
       bool try_pop(T& item);
       size_t size() const;
       bool empty() const;
   private:
       struct alignas(64) Cell {  // cache-line aligned
           std::atomic<size_t> sequence;
           T data;
       };
       std::vector<Cell> buffer_;
       size_t mask_;
       alignas(64) std::atomic<size_t> head_;
       alignas(64) std::atomic<size_t> tail_;
   };
   ```
   - `try_push`: CAS on tail, store data, update sequence
   - `try_pop`: CAS on head, load data, update sequence
   - False sharing prevention via `alignas(64)` padding
3. `include/message.h`:
   - `struct Message { uint64_t id; uint64_t timestamp_ns; std::string topic; std::vector<uint8_t> payload; uint32_t priority; }`
   - `size_t serialized_size() const`
   - `void serialize(uint8_t* buf) const` / `static Message deserialize(const uint8_t* buf, size_t len)`
4. Tests:
   - Single producer, single consumer: push N items, pop N items, verify order
   - Multi-producer (4 threads), single consumer: verify no lost messages
   - Multi-producer (4), multi-consumer (4): verify each message consumed exactly once
   - Full queue: `try_push` returns false
   - Empty queue: `try_pop` returns false

## Phase 2: Topic Management
**Goal:** Implement named topic queues with configurable settings.

### Tasks
1. `include/topic.h` / `src/topic.cpp`:
   - `struct TopicConfig { size_t capacity; size_t max_message_size; RetentionPolicy retention; }`
   - `enum class RetentionPolicy { DROP_OLDEST, REJECT_NEW }`
   - `class Topic`:
     - `Topic(std::string name, TopicConfig config)`
     - `bool publish(Message msg)` — pushes to internal MPMC queue
     - `bool consume(Message& msg)` — pops from queue
     - `size_t pending() const`
     - `const std::string& name() const`
2. `class TopicManager`:
   - `Topic& create_topic(const std::string& name, TopicConfig config)`
   - `Topic& get_topic(const std::string& name)` — throws if not found
   - `void delete_topic(const std::string& name)`
   - `std::vector<std::string> list_topics() const`
   - Thread-safe with `std::shared_mutex` for topic map
3. Tests:
   - Create topic, publish, consume — verify FIFO
   - DROP_OLDEST: publish to full queue overwrites oldest
   - REJECT_NEW: publish to full queue returns false
   - Concurrent publish to same topic from multiple threads

## Phase 3: gRPC Service Definition & Server
**Goal:** Expose the message queue over gRPC with streaming support.

### Tasks
1. `proto/mq.proto`:
   ```protobuf
   syntax = "proto3";
   package mq;

   service MessageQueue {
     rpc Publish(PublishRequest) returns (PublishResponse);
     rpc PublishStream(stream PublishRequest) returns (PublishResponse);
     rpc Subscribe(SubscribeRequest) returns (stream Message);
     rpc CreateTopic(CreateTopicRequest) returns (TopicInfo);
     rpc ListTopics(Empty) returns (ListTopicsResponse);
     rpc GetTopicStats(TopicStatsRequest) returns (TopicStats);
   }

   message PublishRequest { string topic = 1; bytes payload = 2; uint32 priority = 3; }
   message PublishResponse { uint64 message_id = 1; bool success = 2; }
   message SubscribeRequest { string topic = 1; string consumer_group = 2; }
   message Message { uint64 id = 1; string topic = 2; bytes payload = 3; uint64 timestamp_ns = 4; }
   message CreateTopicRequest { string name = 1; uint64 capacity = 2; }
   message TopicInfo { string name = 1; uint64 capacity = 2; }
   message TopicStats { string name = 1; uint64 pending = 2; uint64 published_total = 3; uint64 consumed_total = 4; }
   ```
2. `include/server.h` / `src/server.cpp`:
   - `class MQServer`:
     - Integrates Boost.Asio io_context with gRPC async server
     - `MQServiceImpl` implements `MessageQueue::Service`
     - `Publish`: route message to TopicManager, return message_id
     - `PublishStream`: client-streaming, batch accumulate and publish
     - `Subscribe`: server-streaming, poll topic queue, send messages as they arrive
     - Thread pool: N worker threads running `io_context.run()`
3. `src/server.cpp` — main:
   - Parse CLI args: `--port`, `--threads`, `--default-queue-depth`
   - Build and start gRPC server with Boost.Asio io_context

## Phase 4: Producer & Consumer Clients
**Goal:** Build client libraries and CLI tools.

### Tasks
1. `include/producer_client.h` / `src/producer_client.cpp`:
   - `class ProducerClient`:
     - `ProducerClient(std::string server_addr)`
     - `uint64_t publish(const std::string& topic, const std::vector<uint8_t>& payload)` — sync
     - `void publish_batch(const std::string& topic, const std::vector<std::vector<uint8_t>>& payloads)` — uses streaming RPC
     - `void set_compression(grpc::CompressionAlgorithm algo)`
2. `include/consumer_client.h` / `src/consumer_client.cpp`:
   - `class ConsumerClient`:
     - `ConsumerClient(std::string server_addr, std::string topic, std::string consumer_group)`
     - `void subscribe(std::function<void(const Message&)> callback)` — blocking, invokes callback for each message
     - `void stop()` — cancels subscription
3. `tools/producer_cli.cpp`:
   - CLI: `./producer --server localhost:50051 --topic test --message "hello"` or `--count 1000000` for throughput test
4. `tools/consumer_cli.cpp`:
   - CLI: `./consumer --server localhost:50051 --topic test` — prints messages to stdout

## Phase 5: Benchmarking & Optimization
**Goal:** Achieve 2M+ msg/sec throughput with sub-microsecond queue latency.

### Tasks
1. `benchmarks/bench_queue.cpp`:
   - Benchmark `try_push`/`try_pop` latency (single-threaded)
   - Benchmark MPMC throughput: N producers, M consumers, measure total msg/sec
   - Vary queue sizes: 1K, 64K, 1M entries
2. `benchmarks/bench_throughput.cpp`:
   - End-to-end: producer client → gRPC → server → queue → consumer
   - Measure at message sizes: 64B, 256B, 1KB, 4KB
   - Measure with 1, 4, 8, 16 producer threads
3. Optimization steps:
   - Ensure cache-line padding prevents false sharing (verify with `perf c2c`)
   - Use `memory_order_relaxed` / `memory_order_acquire` / `memory_order_release` precisely
   - Batch SQE submission in gRPC streaming handler
   - Pin threads to CPU cores with `pthread_setaffinity_np`
   - Use huge pages for queue buffer: `mmap` with `MAP_HUGETLB`
4. Create `scripts/run_benchmarks.sh` — runs all benchmarks, outputs results to CSV.
5. `scripts/plot_benchmarks.py` — generates throughput vs thread count and latency distribution charts.
