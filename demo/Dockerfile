FROM golang:1.22 AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && rm -rf /var/lib/apt/lists/*
RUN GOBIN=/usr/local/bin go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.1
WORKDIR /src
COPY demo/go.mod ./demo/
RUN --mount=type=cache,target=/go/pkg/mod cd demo && go mod download && go mod tidy
COPY . .
# Generate protobuf Go code
RUN protoc --version && \
    mkdir -p demo/internal/pb && \
    protoc -I schemas -I /usr/include \
      --go_out=./demo --go_opt=module=github.com/example/event-router-demo \
      schemas/event.proto schemas/testing/person.proto
# Build
RUN --mount=type=cache,target=/go/pkg/mod \
    cd demo && go mod tidy && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/demo ./cmd/demo

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app
COPY --from=build /out/demo /app/demo
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/app/demo"]
