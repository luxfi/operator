# ── Builder ──
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Copy manifest + lock first for layer caching
COPY Cargo.toml Cargo.lock ./
# Dummy src so cargo can resolve deps
RUN mkdir src && echo 'fn main(){}' > src/main.rs
RUN cargo build --release 2>/dev/null || true

# Now copy real source and build
COPY src/ src/
RUN touch src/main.rs && cargo build --release

# ── Runtime ──
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -r -g nogroup -s /sbin/nologin operator

COPY --from=builder /build/target/release/lux-operator /usr/local/bin/lux-operator

USER operator
ENTRYPOINT ["lux-operator"]
