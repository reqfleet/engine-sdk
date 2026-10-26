FROM golang:1.26.9 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOWORK=off
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /usr/local/bin/preparing-adapter . \
    && go test -tags=integration -c -o /usr/local/bin/conformance.test .

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/preparing-adapter /usr/local/bin/preparing-adapter
COPY --from=build /usr/local/bin/conformance.test /usr/local/bin/conformance.test
WORKDIR /app
COPY testdata ./testdata
USER root
ENTRYPOINT ["/usr/local/bin/conformance.test", "-test.run=^TestPublicAdapterRuntime$", "-test.v", "-test.count=1", "-test.timeout=120s"]
