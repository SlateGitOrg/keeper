# One image serves both roles: the manager Deployment and the mover Jobs.
FROM golang:1.27.1-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /keeper ./cmd/keeper

# distroless/static runs as root by default: mover Jobs need root (with every
# capability but DAC_OVERRIDE/CHOWN/FOWNER dropped) to read and restore files
# owned by arbitrary users. The manager Deployment sets runAsUser 65532.
FROM gcr.io/distroless/static-debian12@sha256:6d635b323e6ab633016668144d38e368e2894bd824500369151573225078ee03
COPY --from=build /keeper /keeper
ENTRYPOINT ["/keeper"]
