###############################################

FROM ubuntu:22.04 as builder

RUN apt update -y
RUN apt install -y \
    build-essential \
    clang lld curl

# Disable incremental compilation to avoid overhead. We are not preserving these files anyway.
ENV CARGO_INCREMENTAL="0"
# Disable full debug symbol generation to speed up CI build
# "1" means line tables only, which is useful for panic tracebacks.
ENV CARGO_PROFILE_DEV_DEBUG="1"

# https://github.com/rust-lang/cargo/issues/10280
ENV CARGO_NET_GIT_FETCH_WITH_CLI="true"

# Building job will be killed in docker for using to much ram.
# By using only one job, we limit the ram usage.
# ENV CARGO_BUILD_JOBS="1"

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

COPY . /ol-data

WORKDIR /ol-data

RUN cargo build \
      --profile="release"

RUN strip ./target/release/ol-data

###############################################

FROM ubuntu:22.04 as ol-data

COPY --from=builder /ol-data/target/release/ol-data /bin/ol-data

env PORT="4000"
env CLICKHOUSE_HOST="http://127.0.0.1:8123"
env CLICKHOUSE_USERNAME="default"
env CLICKHOUSE_PASSWORD="default"
env CLICKHOUSE_DATABASE="olfyi"

CMD ["ol-data"]