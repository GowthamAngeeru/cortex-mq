# -------------------------
# STAGE 1: Builder
# -------------------------
FROM rust:1-slim-bullseye as builder

# Install protobuf compiler (Required for your gRPC proto files!)
RUN apt-get update && apt-get install -y protobuf-compiler libprotobuf-dev

WORKDIR /usr/src/cortex-mq

# Copy the ENTIRE project into the builder at once
COPY . .

# Force a clean, linear build of your actual source code
RUN cargo build --release

# -------------------------
# STAGE 2: Runtime
# -------------------------
FROM debian:bullseye-slim

# Install OpenSSL and CA certificates
RUN apt-get update && \
    apt-get install -y openssl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/cortex-mq/target/release/cortex-mq /usr/local/bin/cortex-mq

# Expose the gRPC port
EXPOSE 50051

# Boot the true broker
CMD ["cortex-mq"]