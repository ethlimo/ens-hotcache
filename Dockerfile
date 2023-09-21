FROM rust:1.65 as builder
WORKDIR /usr/src/limo-hotcache
COPY . .
RUN apt-get update && \
    apt-get install -y ca-certificates
RUN cargo install --path . --debug

FROM debian:bullseye-slim
RUN apt-get update && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/limo-hotcache /usr/local/bin/limo-hotcache
COPY --from=builder /etc/ssl /etc/ssl

ENV ETH_RPC_WS=
ENV REDIS_URL=
CMD ["limo-hotcache"]

