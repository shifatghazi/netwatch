# Dockerfile.cpp
# ---------------
# Multi-stage build for the C++ detection engine.
#
# WHY MULTI-STAGE?
#   Stage 1 (builder): has the full compiler toolchain — ~500MB
#   Stage 2 (runtime): has only the compiled binary — ~50MB
#   We copy the binary from stage 1 into stage 2. The final image is 10x
#   smaller, which matters when you're pushing images to a container registry
#   and pulling them onto hundreds of servers.
#   This is a key Docker best practice that interviewers love to hear about.

# ── Stage 1: build ─────────────────────────────────────────────────────────
FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download nlohmann/json single header (MIT license)
RUN mkdir -p /app/third_party && \
    wget -q https://github.com/nlohmann/json/releases/download/v3.11.3/json.hpp \
    -O /app/third_party/json.hpp

COPY CMakeLists.txt /app/
COPY src/ /app/src/

WORKDIR /app/build
RUN cmake .. -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc)

# ── Stage 2: runtime ────────────────────────────────────────────────────────
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/netwatch_engine /usr/local/bin/netwatch_engine

RUN mkdir -p /tmp/netwatch

CMD ["netwatch_engine"]
