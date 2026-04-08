qsys/
├── cmd/                        # 可执行程序入口
│   ├── cli_svr/                # [查询] HTTP Server 入口 
│   ├── upd_svr/                # [更新] 消费 Kafka，双写 DB/Cache 入口 [internal/app]
│   └── sims/                   # [压测与模拟工具] 负责高并发发单
│
├── internal/                   # 私有业务逻辑
│   ├── config/                 # 统一配置解析 (yaml，解析端口、DB密码..)
│   ├── model/                  # 数据结构定义 (Client, Order实体)
│   ├── cache/                  # Redis 封装层，处理 client_id (12位) 列表 [./db]
│   ├── db/                     # MySQL 封装层，处理 exchang_type 和 stock_code
│   ├── mq/                     # Kafka 生产者(tools用)与消费者(upd_svr用)
│   ├── api/                    # HTTP 路由与 Controller (被 cli_svr 调用)
│   └── service/                # 业务层，如原子性更新 
│
├── deploy/                     # 运维与部署
│   ├── nginx.conf              # Nginx 配置文件 (轮询或 ip_hash) 
│   ├── init.sql                # MySQL 建表脚本
│   └── docker-compose.yml      
│
├── wails-dashboard/            # Wails 前端
│   ├── frontend/               # React / Vue / JS ..
│   ├── app.go                  # Wails 绑定的 Go 后端 (调用 tools 发单，或调用 Nginx 查询)
│   └── main.go                 # Wails 桌面程序入口
│
├── go.mod
└── go.sum


Done:
- docker-compose.yml docker的配置
- 初始化db