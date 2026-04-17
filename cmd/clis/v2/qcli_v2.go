package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type setVal struct{}

const (
	metricsPort = "9101"
	concurrency = 3000
	idleTout    = 90 * time.Second
	clientTout  = 5 * time.Second
)

var (
	clientRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "qsys_client_requests_total",
		Help: "Total number of HTTP requests sent by the load tester",
	}, []string{"status", "cache_status"}) // label: · -> cache_status

	clientRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "qsys_client_request_duration_seconds",
		Help:    "Response latency (seconds) from the client pov",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2},
	}, []string{"status", "cache_status"})
)

func init() {
	// 注册指标
	prometheus.MustRegister(clientRequestsTotal)
	prometheus.MustRegister(clientRequestDuration)
}

// genTargetUrl 生成目标 Url
func genTargetUrl(baseUrl string) string {
	var clientId string
	if rand.Float32() < 0.7 {
		baseId := int64(880000000000)
		clientId = fmt.Sprintf("%012d", baseId+rand.Int63n(10000))
	} else {
		baseId := int64(990000000000)
		clientId = fmt.Sprintf("%012d", baseId+rand.Int63n(10000))
	}

	return fmt.Sprintf("%s/api/orders/%s?fmt=j", baseUrl, clientId)
}

func worker(ctx context.Context, client *http.Client, baseUrl string, jobs <-chan setVal) {
	for range jobs {
		url := genTargetUrl(baseUrl)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

		st := time.Now()
		resp, err := client.Do(req)
		t := time.Since(st).Seconds()

		status, cacheRst := "0", "UNKNOWN_ERR"
		if err == nil {
			status = strconv.Itoa(resp.StatusCode)
			cacheRst = resp.Header.Get("X-Cache-Status")
			if cacheRst == "" {
				cacheRst = "MISSING_HEADER"
			}

			// 排空, 关闭 body
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		} else {
			if ctx.Err() == context.DeadlineExceeded {
				cacheRst = "TEST_FINISHED"
			}
			// context deadline exceeded, or broken pipe...
			cacheRst = "NETWORK_FAIL"
		}

		clientRequestsTotal.WithLabelValues(status, cacheRst).Inc()
		clientRequestDuration.WithLabelValues(status, cacheRst).Observe(t)
	}
}

func main() {
	rateParam := flag.Int("rate", 3000, "QPS/每秒请求数")
	timeParam := flag.Duration("time", 160*time.Second, "压测持续时间")
	tarUrl := flag.String("url", "http://localhost", "Nginx 网关地址")
	flag.Parse()

	// 启动 Metrics 服务
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Metrics service ok: http://localhost:%s/metrics", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
			log.Fatalf("Failed to start metrics service: %v", err)
		}
	}()

	// 初始化 Client
	c := concurrency
	myTransport := &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 2000,
		MaxConnsPerHost:     3 * c,
		IdleConnTimeout:     idleTout,
		DisableKeepAlives:   false,
	}
	client := &http.Client{
		Transport: myTransport,
		Timeout:   clientTout,
	}

	fmt.Println("Qsys http 压测启动 2..")
	ctx, cancel := context.WithTimeout(context.Background(), *timeParam)
	defer cancel()

	// 启动 worker pool
	jobs := make(chan setVal, c)
	for range c {
		go worker(ctx, client, *tarUrl, jobs)
	}

	tk := time.NewTicker(time.Second / time.Duration(*rateParam))
	defer tk.Stop()

Loop:
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("到达设定时限 %v\n", *timeParam)
			break Loop
		case <-tk.C:
			select {
			case jobs <- setVal{}:
			default:
				log.Println("Warning! Worker pool 满载, 请求堆积...")
			}
		}
	}

	time.Sleep(3 * time.Second)
	fmt.Println("Done.")
}
