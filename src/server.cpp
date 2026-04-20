#include <grpcpp/grpcpp.h>
#include <grpcpp/server_builder.h>

#include "blitz_queue/mpmc_queue.hpp"
#include "queue.grpc.pb.h"
#include "queue.pb.h"

#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;

using blitzqueue::CreateQueueRequest;
using blitzqueue::CreateQueueResponse;
using blitzqueue::DequeueRequest;
using blitzqueue::DequeueResponse;
using blitzqueue::EnqueueRequest;
using blitzqueue::EnqueueResponse;
using blitzqueue::StatsRequest;
using blitzqueue::StatsResponse;
using blitzqueue::QueueService;

// ─────────────────────────────────────────────────────────────────────────────
// Named queue entry
// ─────────────────────────────────────────────────────────────────────────────

static constexpr std::size_t kQueueCapacity = 65536; // 64 K slots per queue

struct QueueEntry {
    blitz::MpmcQueue<std::string, kQueueCapacity> queue;
    std::atomic<uint64_t> total_enqueued{0};
    std::atomic<uint64_t> total_dequeued{0};

    QueueEntry() = default;
    // non-copyable / non-movable because MpmcQueue contains atomics
    QueueEntry(const QueueEntry&) = delete;
    QueueEntry& operator=(const QueueEntry&) = delete;
};

// ─────────────────────────────────────────────────────────────────────────────
// Service implementation
// ─────────────────────────────────────────────────────────────────────────────

class QueueServiceImpl final : public QueueService::Service {
public:
    Status CreateQueue(ServerContext* /*ctx*/,
                       const CreateQueueRequest* req,
                       CreateQueueResponse* resp) override {
        const auto& name = req->queue_name();
        if (name.empty()) {
            resp->set_success(false);
            resp->set_error("queue_name must not be empty");
            return Status::OK;
        }
        std::unique_lock lock(map_mu_);
        if (queues_.find(name) == queues_.end()) {
            queues_.emplace(name, std::make_unique<QueueEntry>());
        }
        resp->set_success(true);
        return Status::OK;
    }

    Status Enqueue(ServerContext* /*ctx*/,
                   const EnqueueRequest* req,
                   EnqueueResponse* resp) override {
        std::shared_lock lock(map_mu_);
        auto it = queues_.find(req->queue_name());
        if (it == queues_.end()) {
            resp->set_success(false);
            resp->set_error("Queue not found: " + req->queue_name());
            return Status::OK;
        }
        QueueEntry* entry = it->second.get();
        lock.unlock();

        std::string payload = req->payload();
        bool ok = entry->queue.enqueue(std::move(payload));
        if (ok) {
            entry->total_enqueued.fetch_add(1, std::memory_order_relaxed);
            resp->set_success(true);
        } else {
            resp->set_success(false);
            resp->set_error("Queue full");
        }
        return Status::OK;
    }

    Status Dequeue(ServerContext* /*ctx*/,
                   const DequeueRequest* req,
                   DequeueResponse* resp) override {
        std::shared_lock lock(map_mu_);
        auto it = queues_.find(req->queue_name());
        if (it == queues_.end()) {
            resp->set_success(false);
            resp->set_error("Queue not found: " + req->queue_name());
            return Status::OK;
        }
        QueueEntry* entry = it->second.get();
        lock.unlock();

        std::string item;
        uint32_t timeout_ms = req->timeout_ms();
        uint32_t waited = 0;
        constexpr uint32_t kSleepUs = 100;

        do {
            if (entry->queue.dequeue(item)) {
                entry->total_dequeued.fetch_add(1, std::memory_order_relaxed);
                resp->set_success(true);
                resp->set_payload(std::move(item));
                return Status::OK;
            }
            if (timeout_ms > 0) {
                struct timespec ts{0, kSleepUs * 1000};
                nanosleep(&ts, nullptr);
                waited += kSleepUs / 1000;
            }
        } while (waited < timeout_ms);

        resp->set_success(false);
        resp->set_error("Queue empty");
        return Status::OK;
    }

    Status Stats(ServerContext* /*ctx*/,
                 const StatsRequest* req,
                 StatsResponse* resp) override {
        std::shared_lock lock(map_mu_);
        auto it = queues_.find(req->queue_name());
        resp->set_queue_name(req->queue_name());
        if (it == queues_.end()) {
            resp->set_exists(false);
            return Status::OK;
        }
        QueueEntry* entry = it->second.get();
        resp->set_exists(true);
        resp->set_current_size(static_cast<uint64_t>(entry->queue.size()));
        resp->set_capacity(static_cast<uint64_t>(kQueueCapacity));
        resp->set_total_enqueued(
            entry->total_enqueued.load(std::memory_order_relaxed));
        resp->set_total_dequeued(
            entry->total_dequeued.load(std::memory_order_relaxed));
        return Status::OK;
    }

private:
    mutable std::shared_mutex map_mu_;
    std::unordered_map<std::string, std::unique_ptr<QueueEntry>> queues_;
};

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    std::string addr = "0.0.0.0:50051";
    if (argc > 1) addr = argv[1];

    QueueServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    builder.SetMaxReceiveMessageSize(64 * 1024 * 1024);
    builder.SetMaxSendMessageSize(64 * 1024 * 1024);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "Failed to start server on " << addr << "\n";
        return 1;
    }
    std::cout << "BlitzQueue gRPC server listening on " << addr << "\n";
    server->Wait();
    return 0;
}
