# syntax=docker/dockerfile:1

# One image, every binary: the fleet differs only in which program it runs,
# so building eleven images would be eleven copies of the same link step.
# The deployment names the binary (command: ["/usr/bin/dispatcher"]), which
# is why this file deliberately sets no ENTRYPOINT.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Modules first, so editing a source file does not re-download them.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Static and unprivileged: pgx, franz-go and the rest are pure Go, so no
# libc and no cgo are needed - and a static binary is what makes the
# distroless static base below possible.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# distroless/static: no shell, no package manager, no libc - the smallest
# thing that can still run a static binary and talk TLS (it carries the CA
# bundle the dispatcher needs for webhook callbacks). The nonroot tag runs
# everything as uid 65532; every service binds a port above 1024, so nothing
# needs privileges.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/bin/
