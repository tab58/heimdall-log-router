# syntax=docker/dockerfile:1.7

# ---- Stage 1: build the Heimdall binary ----
FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY heimdall ./heimdall
COPY pkg ./pkg

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath -ldflags='-s -w' \
    -o /out/heimdall ./heimdall/cmd/app

# ---- Stage 2: final image (Vector base image, Debian-slim under the hood) ----
FROM timberio/vector:0.44.0-debian

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder --chmod=0755 /out/heimdall /usr/local/bin/heimdall
COPY --chmod=0755 docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENV HEIMDALL_CONFIG_PATH=/etc/heimdall/heimdall.yaml \
    HEIMDALL_SERVER_PORT=7077

EXPOSE 7077

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS "http://localhost:${HEIMDALL_SERVER_PORT}/healthz" || exit 1

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
