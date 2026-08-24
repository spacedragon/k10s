FROM rust:1.97-slim-bookworm AS builder

ARG TRUNK_VERSION=0.21.14
ARG WASM_BINDGEN_VERSION=0.2.127
RUN apt-get update \
    && apt-get install -y --no-install-recommends binaryen ca-certificates curl pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add wasm32-unknown-unknown \
    && curl -fsSL "https://github.com/trunk-rs/trunk/releases/download/v${TRUNK_VERSION}/trunk-x86_64-unknown-linux-gnu.tar.gz" \
       | tar -xz -C /usr/local/bin trunk \
    && curl --retry 5 --retry-all-errors -fsSL \
       "https://github.com/rustwasm/wasm-bindgen/releases/download/${WASM_BINDGEN_VERSION}/wasm-bindgen-${WASM_BINDGEN_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
       | tar -xz -C /tmp \
    && install -m 0755 "/tmp/wasm-bindgen-${WASM_BINDGEN_VERSION}-x86_64-unknown-linux-musl/wasm-bindgen" /usr/local/bin/wasm-bindgen

WORKDIR /src
COPY . .
RUN trunk build --release \
    && cargo build --locked --release -p k10s-server-app --bin k10s-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home --home-dir /home/k10s k10s
COPY --from=builder /src/target/release/k10s-server /usr/local/bin/k10s-server
COPY packaging/container/entrypoint.sh /usr/local/bin/k10s-entrypoint
RUN chmod 0755 /usr/local/bin/k10s-server /usr/local/bin/k10s-entrypoint
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/k10s-entrypoint"]
