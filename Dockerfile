FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /agent ./cmd/agent

FROM alpine:3.19
COPY --from=builder /agent /usr/local/bin/agent
RUN adduser -D -u 1000 agent
USER agent
ENTRYPOINT ["agent"]
