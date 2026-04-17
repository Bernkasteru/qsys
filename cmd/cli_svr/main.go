package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"qsys/internal/config"
	"qsys/internal/db"
	"qsys/internal/model"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	frecover "github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
	"google.golang.org/protobuf/proto"
)

var clientIdRegex = regexp.MustCompile(`^\d{12}$`)

type CliEngine struct {
	insName string
	cfg     *config.Config
	mysqlDB *db.OrderRepo
	redisDB *db.RedisRepo
	app     *fiber.App
}

const (
	rdTout   = 6 * time.Second
	wtTout   = 6 * time.Second
	idleTout = 40 * time.Second
	genTTL   = 2 * time.Second
	sdTout   = 5 * time.Second
)

const (
	cstPath   = "/api/orders/:client_id"
	cstMethod = "GET"
)

// Prometheus 监控指标
var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "qsys_http_requests_total",
		Help: "Total number of HTTP requests",
	}, []string{"method", "path", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "qsys_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2},
	}, []string{"method", "path"})
)

func NewCliEngine(cfgPath string) (*CliEngine, error) {
	insName := os.Getenv("SERVER_NAME")
	if insName == "" {
		insName = "qsys-cli-unknown"
	}

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to load cfg: %w", err)
	}

	mRepo, err := db.NewOrderRepo(cfg.MySQL.DSN)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect mysql: %w", err)
	}
	rRepo := db.NewRedisRepo(db.RedisConfig{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		PoolSize: 64,
	})

	app := fiber.New(fiber.Config{
		AppName:      "Qsys-QueryApi", // 对内标识
		ServerHeader: "Qsys-Gateway",  // 对外...
		ReadTimeout:  rdTout,
		WriteTimeout: wtTout,
		IdleTimeout:  idleTout,
		// 适配 Nginx
		ProxyHeader: "X-Forwarded-For",
		TrustProxy:  true,
		// 性能优化
		ReduceMemoryUsage: true,
	})

	e1 := &CliEngine{
		insName: insName,
		cfg:     cfg,
		mysqlDB: mRepo,
		redisDB: rRepo,
		app:     app,
	}

	e1.setupApp()
	return e1, nil
}

func (e *CliEngine) setupApp() {
	// 容错中间件
	e.app.Use(frecover.New(frecover.Config{
		EnableStackTrace: true,
	}))

	// /metrics 接口, 供 Prometheus 拉取
	promHandler := fasthttpadaptor.NewFastHTTPHandler(promhttp.Handler())
	e.app.Get("/metrics", func(c fiber.Ctx) error {
		promHandler(c.RequestCtx())
		return nil
	})
	// 日志追踪 & Prometheus 监控中间件
	e.app.Use(func(c fiber.Ctx) error {
		st := time.Now()
		err := c.Next()

		// 修复奇怪的错误情况下 Http200
		code := c.Response().StatusCode()
		if err != nil {
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			} else {
				code = fiber.StatusInternalServerError // 未知错误? 500
			}
		}
		path := c.Path()
		log.Printf("[%s] %s, %d, %s, %s; ip: %s", e.insName, c.Method(), code, path, time.Since(st), c.IP())

		if path != "/metrics" && path != "/ping" { // Prometheus 指标上报
			routePath := c.Route().Path
			if routePath == "" {
				routePath = "unknown" // 兜底处理
			}
			t := time.Since(st).Seconds()
			httpRequestsTotal.WithLabelValues(cstMethod, cstPath, strconv.Itoa(code)).Inc()
			httpRequestDuration.WithLabelValues(cstMethod, cstPath).Observe(t)
		}

		return err
	})

	// ping 检查
	e.app.Get("/ping", func(c fiber.Ctx) error {
		return c.SendString("pong")
	})
	// 业务路由
	e.app.Get("/api/orders/:client_id", e.handleQueryOrder)
}

func (e *CliEngine) handleQueryOrder(c fiber.Ctx) error {
	clientId := c.Params("client_id")
	if !clientIdRegex.MatchString(clientId) { // 校验格式
		return c.Status(fiber.StatusBadRequest).SendString("Invalid client_id")
	}

	fmtType := c.Query("fmt", "p")
	if fmtType != "p" && fmtType != "j" {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid format param; use ?fmt=p or ?fmt=j")
	}

	ctx, cancel := context.WithTimeout(c.Context(), genTTL)
	defer cancel()

	// Redis 判断是否活跃
	isActive, err := e.redisDB.SIsMember(ctx, clientId)
	if err != nil {
		log.Printf("[%s] Redis/cache err: %v", e.insName, err)
		c.Set("X-Cache-Status", "CACHE_ERR")
		return c.Status(fiber.StatusInternalServerError).SendString("Cache error")
	}
	if !isActive { // 拦截无效 req
		c.Set("X-Cache-Status", "SHORT_CIRCUIT")
		return sendResp(c, &model.QueryResp{
			ClientId: clientId,
			Infos:    []*model.OrderInfo{},
		}, fmtType)
	}

	// 查 mysql
	c.Set("X-Cache-Status", "HIT_ACTIVE")
	orders, err := e.mysqlDB.GetOrders(ctx, clientId)
	if err != nil {
		log.Printf("[%s] Mysql err: %v", e.insName, err)
		return c.Status(fiber.StatusInternalServerError).SendString("DB error")
	}

	// 构造响应
	infoSlc := make([]*model.OrderInfo, len(orders))
	for i, order := range orders {
		infoSlc[i] = &model.OrderInfo{
			ExchangeType: order.ExchangeType,
			StockCode:    order.StockCode,
		}
	}

	return sendResp(c, &model.QueryResp{
		ClientId: clientId,
		Infos:    infoSlc,
	}, fmtType)
}

func sendProtoResp(c fiber.Ctx, resp *model.QueryResp) error {
	data, err := proto.Marshal(resp)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Proto encode error")
	}
	c.Set("Content-Type", "application/x-protobuf")
	return c.Send(data)
}

// sendResp 统一处理响应
func sendResp(c fiber.Ctx, resp *model.QueryResp, fmtType string) error {
	if fmtType == "j" {
		return c.JSON(resp)
	}
	return sendProtoResp(c, resp)
}

func (e *CliEngine) Start() {
	listenAddr := ":" + strconv.Itoa(e.cfg.App.Port)
	listenCfg := fiber.ListenConfig{
		DisableStartupMessage: e.cfg.App.Env == "prod",
	}
	log.Printf("[%s] Starting api server on %s..", e.insName, listenAddr)
	if err := e.app.Listen(listenAddr, listenCfg); err != nil {
		log.Fatalf("[%s] Listen err: %v", e.insName, err)
	}
}

func (e *CliEngine) Close() {
	sdCtx, cancel := context.WithTimeout(context.Background(), sdTout)
	defer cancel()
	if err := e.app.ShutdownWithContext(sdCtx); err != nil {
		log.Printf("[%s] Shutdown err: %v", e.insName, err)
	}

	_ = e.mysqlDB.Close()
	_ = e.redisDB.Close()
	log.Println("[Cli_main] Engine shutdown")
}

func main() {
	cfgPath := "./deploy/config.yml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	e1, err := NewCliEngine(cfgPath)
	if err != nil {
		log.Fatalf("[Cli_main] Init err: %v", err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go e1.Start()
	<-sigChan
	log.Println("[Cli_main] Shutdown signal received")
	e1.Close()
}
