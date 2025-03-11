FROM rust:1.85-slim AS chef
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev cmake g++ curl protobuf-compiler && \
    rm -rf /var/lib/apt/lists/* && \
    cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release

# Create a smaller runtime image
FROM debian:bullseye-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y libssl1.1 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/rust-connect /app/rust-connect
COPY --from=builder /app/config /app/config

# Set the entrypoint
ENTRYPOINT ["/app/rust-connect"]
