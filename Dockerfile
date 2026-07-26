# syntax=docker/dockerfile:1

ARG RHIZA_PROFILE=sql

FROM rust:1.95-trixie AS builder
ARG RHIZA_PROFILE
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        libclang-dev \
        libssl-dev \
        pkg-config \
        python3 \
    && rm -rf /var/lib/apt/lists/*
ENV LBUG_BUILD_FROM_SOURCE=1
WORKDIR /src
COPY . .
RUN --mount=type=cache,id=rhiza-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rhiza-cargo-target,target=/src/target,sharing=locked \
    case "$RHIZA_PROFILE" in \
      sql|graph) \
        cargo build --release --locked -p rhiza-cli --bin rhiza \
          --no-default-features --features "$RHIZA_PROFILE,recorder-postcard-rpc" \
        ;; \
      *) echo "RHIZA_PROFILE must be sql|graph" >&2; \
        exit 64 \
        ;; \
    esac \
    && install -D -m 0755 /src/target/release/rhiza /out/rhiza

FROM debian:trixie-slim
ARG RHIZA_PROFILE
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3t64 libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/rhiza /usr/local/bin/rhiza
LABEL io.rhiza.build-profile="$RHIZA_PROFILE"
ENTRYPOINT ["rhiza"]
