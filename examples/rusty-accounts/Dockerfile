# ---------- BUILD STAGE ---------- #

FROM rust:1.77.2-bookworm AS builder

# Pass build arguments and set environment variables.
ARG PROFILE=release

WORKDIR /opt

# Configure cargo to use the git CLI to use the provided GITHUB_TOKEN for private repositories.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

# RUN --mount=type=secret,id=GITHUB_TOKEN \
#   export GITHUB_TOKEN=$(cat /run/secrets/GITHUB_TOKEN) && \
#   git config --global url."https://$GITHUB_TOKEN@github.com".insteadOf "ssh://git@github.com"

# Install protobuf-compiler.
# RUN apt-get update && apt-get install -y protobuf-compiler

# Copy minimal set of project files for the below dummy build.
COPY ./Cargo.toml ./Cargo.lock ./

# Create a dummy main.rs to leverage dependency caching.
# This avoids reinstalling all dependencies if only the source code changes.
# The final find -exec touch command pretends that we built a long time ago, so any changes
# introduced by COPY ./src are actually built again
RUN mkdir src && \
  echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs && \
  touch -t 197001010001 src src/main.rs && \
  cargo build --locked --profile $PROFILE && \
  find ./target -exec touch -t 197001010002 -m {} +

# Copy the actual source after the dummy build to ensure dependencies are cached.
COPY ./src        ./src
# COPY ./migrations ./migrations

# Build the actual application.
RUN cargo build --locked --profile $PROFILE && \
  mv ./target/$([ "$PROFILE" = "release" ] && echo "release" || echo "debug")/rusty-accounts /

# ---------- RUN STAGE ---------- #

FROM debian:bookworm-slim AS final

WORKDIR /opt

# Create a non-root user for running the application.
RUN adduser --disabled-password --gecos "" --home "/nonexistent" \
  --shell "/sbin/nologin" --no-create-home --uid "10001" appuser

# Copy the binary and change its ownership at the same time.
COPY --from=builder --chown=appuser:appuser /rusty-accounts /opt/rusty-accounts

# Use the non-root user to run the application.
USER appuser

# Set the entry point for the container.
ENTRYPOINT ["/opt/rusty-accounts"]
