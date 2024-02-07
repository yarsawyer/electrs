FROM rust:latest AS base

RUN apt update -qy
RUN apt install -qy librocksdb-dev


FROM base as builder

WORKDIR /usr/src/app
RUN apt install -qy git cargo clang cmake libssl-dev
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY rust-bellcoin rust-bellcoin
RUN cargo build --release


FROM base as runner

COPY --from=builder /usr/src/app/target/release/electrs /bin/electrs
