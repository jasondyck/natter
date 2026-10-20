# Server binary (with aws-lc for DTLS: faster handshakes when many join at
# once) plus the built client, on a slim Debian base.
#
#   docker build -t natter .

FROM rust:1.95-trixie AS build
ARG TRUNK_VERSION=0.21.14
RUN rustup target add wasm32-unknown-unknown \
    && curl -fsSL "https://github.com/trunk-rs/trunk/releases/download/v${TRUNK_VERSION}/trunk-x86_64-unknown-linux-gnu.tar.gz" \
        | tar -xz -C /usr/local/bin
WORKDIR /src
COPY . .
# Cache mounts keep crates and incremental output between builds; the
# results are copied out because the mounts vanish after each RUN.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p natter-server --features aws-lc \
    && cp target/release/natter-server /natter-server
# `--release` is what makes trunk run wasm-opt. It fetches wasm-bindgen and
# wasm-opt itself, at the versions Cargo.lock needs.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cd crates/client && trunk build --release --locked \
    && cp -r dist /dist

FROM debian:trixie-slim
COPY --from=build /natter-server /usr/local/bin/natter-server
COPY --from=build /dist /app/dist
# The volume starts as this directory, so the server's user must own it.
RUN mkdir /data && chown 10001:10001 /data
ENV NATTER_STATIC_DIR=/app/dist \
    NATTER_DATA_DIR=/data \
    NATTER_HTTP_ADDR=0.0.0.0:8080 \
    NATTER_UDP_BIND=0.0.0.0
# NATTER_ORIGIN and NATTER_PUBLIC_IP are deployment-specific; see README.
USER 10001:10001
VOLUME /data
EXPOSE 8080/tcp 50000/udp 50000/tcp
ENTRYPOINT ["/usr/local/bin/natter-server"]
