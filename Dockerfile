# syntax=docker/dockerfile:1.7

FROM rust:1.88-bookworm AS builder

WORKDIR /src
COPY . .
RUN cargo build --locked --release --bin ermya-graph-server

FROM debian:bookworm-slim AS runtime

ARG SOURCE_REVISION
LABEL org.opencontainers.image.title="Ermya Graph Community" \
      org.opencontainers.image.version="0.13.2" \
      org.opencontainers.image.revision=$SOURCE_REVISION \
      org.opencontainers.image.source="https://github.com/mojobytes/ermya-graph"

RUN groupadd --system --gid 10001 ermya \
    && useradd --system --uid 10001 --gid ermya --home-dir /var/lib/ermya ermya \
    && install -d -o ermya -g ermya /var/lib/ermya /etc/ermya/tls

COPY --from=builder /src/target/release/ermya-graph-server /usr/local/bin/ermya-graph-server

ENV ERMYA_BIND=0.0.0.0:7687 \
    ERMYA_DATA_DIR=/var/lib/ermya \
    ERMYA_TLS_CERT=/etc/ermya/tls/server.crt \
    ERMYA_TLS_KEY=/etc/ermya/tls/server.key

VOLUME ["/var/lib/ermya"]
EXPOSE 7687

USER ermya
ENTRYPOINT ["/usr/local/bin/ermya-graph-server"]
