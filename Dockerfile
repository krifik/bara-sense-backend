# ==========================================
# Dockerfile Backend BARA-Sense
# (Debian Bookworm - Tanpa Alpine)
# ==========================================

# ------------------------------------------
# Stage 1: Build Backend (Go on Debian Bookworm)
# ------------------------------------------
FROM golang:1.26-bookworm AS backend-builder
WORKDIR /app/backend

# Install git & certificates jika diperlukan modul eksternal
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates tzdata && rm -rf /var/lib/apt/lists/*

# Cache go modules
COPY backend/go.mod backend/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy backend source code
COPY backend/ ./

# Build statically-linked binary
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o server main.go tuya_history.go

# ------------------------------------------
# Stage 3: Debian Slim Production Runner
# ------------------------------------------
FROM debian:bookworm-slim AS runner
WORKDIR /app

# Install runtime ca-certificates, tzdata, dan curl (glibc standar)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    tzdata \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -fs /usr/share/zoneinfo/Asia/Jakarta /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata

# Buat direktori logs, frontend dist, dan docs
RUN mkdir -p /app/logs /app/frontend/dist /app/docs

# Copy binary dari stage backend-builder
COPY --from=backend-builder /app/backend/server /app/server

# Copy swagger docs jika ada
COPY --from=backend-builder /app/backend/docs /app/docs/

# Expose default HTTP/WebSocket port
EXPOSE 3000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:3000/api/devices || exit 1

# Jalankan aplikasi
ENTRYPOINT ["/app/server"]
