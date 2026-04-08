#  - 编译阶段
FROM golang:1.26-alpine AS builder

WORKDIR /app

ENV GOPROXY=https://goproxy.cn,direct
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o cli_svr ./cmd/cli_svr/main.go


#  - 运行阶段
FROM alpine:3.21.2

# 设置时区
RUN apk add --no-cache tzdata && \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

WORKDIR /app

COPY --from=builder /app/cli_svr .
COPY --from=builder /app/deploy /app/deploy

# 服务端口
EXPOSE 8080

CMD ["./cli_svr", "./deploy/config.yml"]