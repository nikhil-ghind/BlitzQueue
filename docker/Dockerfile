FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

# ── System dependencies ───────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    git \
    ca-certificates \
    # Boost
    libboost-system-dev \
    libboost-thread-dev \
    # gRPC / Protobuf
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler \
    protobuf-compiler-grpc \
 && rm -rf /var/lib/apt/lists/*

# ── Non-root user ─────────────────────────────────────────────────────────────
RUN useradd -m -u 1001 blitz
WORKDIR /app
COPY --chown=blitz:blitz . .

# ── Build ─────────────────────────────────────────────────────────────────────
USER blitz
RUN cmake -S . -B build \
      -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build --parallel $(nproc)

# ── Runtime ───────────────────────────────────────────────────────────────────
EXPOSE 50051
CMD ["./build/blitz-server", "0.0.0.0:50051"]
