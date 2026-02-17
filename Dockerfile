FROM rust:1.93-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

CMD ["bash", "-lc", "ls -lah target/release/ && echo 'Plugin artifact built in target/release'"]
