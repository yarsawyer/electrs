FROM rust:latest AS base

RUN apt update -qy
RUN apt install -qy librocksdb-dev


FROM base as builder

WORKDIR /usr/src/app
RUN apt install -qy git cargo clang cmake libssl-dev
RUN rustup target add x86_64-unknown-linux-musl
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY rust-bellcoin rust-bellcoin
RUN cargo build --target x86_64-unknown-linux-musl --release


FROM base as runner

COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/electrs /bin/electrs
