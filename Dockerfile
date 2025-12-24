# Build stage
FROM rust:1.75-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "pub fn dummy() {}" > src/lib.rs

# Build dependencies only (this layer will be cached)
RUN cargo build --release && rm -rf src

# Copy actual source code
COPY src ./src

# Touch main.rs to ensure rebuild
RUN touch src/main.rs src/lib.rs

# Build the actual application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/axum-rtmp-server /app/axum-rtmp-server

# Copy default config
COPY config.toml /app/config.toml

# Expose RTMP and HTTP metrics ports
EXPOSE 1935 8080

# Run the server
CMD ["./axum-rtmp-server"]
