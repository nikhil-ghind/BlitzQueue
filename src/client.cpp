#include <grpcpp/grpcpp.h>

#include "queue.grpc.pb.h"
#include "queue.pb.h"

#include <iostream>
#include <memory>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

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
// Client wrapper
// ─────────────────────────────────────────────────────────────────────────────

class BlitzClient {
public:
    explicit BlitzClient(const std::string& addr)
        : stub_(QueueService::NewStub(
              grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))) {}

    bool create(const std::string& queue) {
        CreateQueueRequest req;
        req.set_queue_name(queue);
        CreateQueueResponse resp;
        ClientContext ctx;
        Status s = stub_->CreateQueue(&ctx, req, &resp);
        if (!s.ok()) { std::cerr << "RPC error: " << s.error_message() << "\n"; return false; }
        if (!resp.success()) { std::cerr << "Error: " << resp.error() << "\n"; return false; }
        std::cout << "Queue '" << queue << "' created.\n";
        return true;
    }

    bool enqueue(const std::string& queue, const std::string& payload) {
        EnqueueRequest req;
        req.set_queue_name(queue);
        req.set_payload(payload);
        EnqueueResponse resp;
        ClientContext ctx;
        Status s = stub_->Enqueue(&ctx, req, &resp);
        if (!s.ok()) { std::cerr << "RPC error: " << s.error_message() << "\n"; return false; }
        if (!resp.success()) { std::cerr << "Error: " << resp.error() << "\n"; return false; }
        std::cout << "Enqueued to '" << queue << "': " << payload << "\n";
        return true;
    }

    bool dequeue(const std::string& queue, uint32_t timeout_ms = 0) {
        DequeueRequest req;
        req.set_queue_name(queue);
        req.set_timeout_ms(timeout_ms);
        DequeueResponse resp;
        ClientContext ctx;
        Status s = stub_->Dequeue(&ctx, req, &resp);
        if (!s.ok()) { std::cerr << "RPC error: " << s.error_message() << "\n"; return false; }
        if (!resp.success()) { std::cerr << "Queue empty or error: " << resp.error() << "\n"; return false; }
        std::cout << resp.payload() << "\n";
        return true;
    }

    bool stats(const std::string& queue) {
        StatsRequest req;
        req.set_queue_name(queue);
        StatsResponse resp;
        ClientContext ctx;
        Status s = stub_->Stats(&ctx, req, &resp);
        if (!s.ok()) { std::cerr << "RPC error: " << s.error_message() << "\n"; return false; }
        if (!resp.exists()) { std::cerr << "Queue '" << queue << "' does not exist.\n"; return false; }
        std::cout << "Queue:           " << resp.queue_name() << "\n"
                  << "Current size:    " << resp.current_size() << "\n"
                  << "Capacity:        " << resp.capacity() << "\n"
                  << "Total enqueued:  " << resp.total_enqueued() << "\n"
                  << "Total dequeued:  " << resp.total_dequeued() << "\n";
        return true;
    }

private:
    std::unique_ptr<QueueService::Stub> stub_;
};

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────

static void print_usage(const char* prog) {
    std::cerr << "Usage:\n"
              << "  " << prog << " [--server <addr>] create  <queue>\n"
              << "  " << prog << " [--server <addr>] enqueue <queue> <message>\n"
              << "  " << prog << " [--server <addr>] dequeue <queue> [--timeout-ms <ms>]\n"
              << "  " << prog << " [--server <addr>] stats   <queue>\n"
              << "\nDefault server: localhost:50051\n";
}

int main(int argc, char* argv[]) {
    if (argc < 2) { print_usage(argv[0]); return 1; }

    std::string server = "localhost:50051";
    int idx = 1;

    // Parse optional --server flag
    if (std::string(argv[idx]) == "--server") {
        if (idx + 1 >= argc) { std::cerr << "Missing argument for --server\n"; return 1; }
        server = argv[idx + 1];
        idx += 2;
    }

    if (idx >= argc) { print_usage(argv[0]); return 1; }

    BlitzClient client(server);
    std::string cmd = argv[idx++];

    if (cmd == "create") {
        if (idx >= argc) { std::cerr << "Missing queue name\n"; return 1; }
        return client.create(argv[idx]) ? 0 : 1;

    } else if (cmd == "enqueue") {
        if (idx + 1 >= argc) { std::cerr << "Usage: enqueue <queue> <message>\n"; return 1; }
        std::string q = argv[idx++];
        std::string msg = argv[idx];
        return client.enqueue(q, msg) ? 0 : 1;

    } else if (cmd == "dequeue") {
        if (idx >= argc) { std::cerr << "Missing queue name\n"; return 1; }
        std::string q = argv[idx++];
        uint32_t timeout_ms = 0;
        if (idx + 1 < argc && std::string(argv[idx]) == "--timeout-ms") {
            timeout_ms = static_cast<uint32_t>(std::stoul(argv[idx + 1]));
        }
        return client.dequeue(q, timeout_ms) ? 0 : 1;

    } else if (cmd == "stats") {
        if (idx >= argc) { std::cerr << "Missing queue name\n"; return 1; }
        return client.stats(argv[idx]) ? 0 : 1;

    } else {
        std::cerr << "Unknown command: " << cmd << "\n";
        print_usage(argv[0]);
        return 1;
    }
}
