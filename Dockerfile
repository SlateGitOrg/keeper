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
FROM gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2
COPY --from=build /keeper /keeper
ENTRYPOINT ["/keeper"]
