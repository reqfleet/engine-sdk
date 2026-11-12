# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.9-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
ENV GOWORK=off CGO_ENABLED=0
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go test -c -tags=integration -o /out/conformance-runtime ./conformance

FROM debian:bookworm-slim
RUN mkdir -p /test-data && chown 65532:65532 /test-data
COPY --from=build /out/conformance-runtime /usr/local/bin/conformance-runtime
USER 0:0
ENTRYPOINT ["/usr/local/bin/conformance-runtime", "-test.v", "-test.timeout=3m"]
