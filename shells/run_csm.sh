#!/bin/bash

echo ">>> 1. 清理所有残留进程"
pkill -9 qsys_consumer 2>/dev/null 

echo ">>> 2. 重新编译代码"
go build -o qsys_consumer cmd/consumer/main.go

echo ">>> 3. 启动 3 个测试节点"
# QSYS_TEST_MODE=1 ./qsys_consumer deploy/config.yml &
# QSYS_TEST_MODE=1 ./qsys_consumer deploy/config.yml &
# QSYS_TEST_MODE=1 ./qsys_consumer deploy/config.yml &
./qsys_consumer deploy/config.yml &
./qsys_consumer deploy/config.yml &
./qsys_consumer deploy/config.yml &
