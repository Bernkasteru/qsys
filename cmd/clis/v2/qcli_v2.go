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
	"golang.org/x/time/rate"
)

type setVal struct{}

// 地域模拟 IP
type RegionIP struct {
	Name   string
	Prefix string
}

var cnRegions = []RegionIP{
	{"Beijing", "114.247"}, {"Shanghai", "101.224"}, {"Guangdong", "113.64"},
	{"Zhejiang", "115.192"}, {"Jiangsu", "112.112"}, {"Sichuan", "125.64"},
	{"Hubei", "119.96"}, {"Shandong", "113.120"},
}

var frRegions = []RegionIP{
	{"US", "29.8"}, {"JP", "133.200"}, {"SG", "118.200"},
}

const (
	metricsPort = "9101"
	concurrency = 10000
	idleTout    = 90 * time.Second
	clientTout  = 5 * time.Second
)

var (
	clientRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "qsys_client_requests_total",
		Help: "Total number of HTTP requests sent by the load tester",
	}, []string{"status", "cache_status", "region"}) // label: · -> cache_status

	clientRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "qsys_client_request_duration_seconds",
		Help:    "Response latency (seconds) from the client pov",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2},
	}, []string{"status", "cache_status", "region"})
)

func init() {
	// 注册指标
	prometheus.MustRegister(clientRequestsTotal)
	prometheus.MustRegister(clientRequestDuration)
}

func getSimPair() (string, string) {
	r := rand.Float32()
	var region RegionIP
	if r < 0.9 {
		// 90% 为中国 IP
		region = cnRegions[rand.Intn(len(cnRegions))]
	} else {
		region = frRegions[rand.Intn(len(frRegions))]
	}
	ip := fmt.Sprintf("%s.%d.%d", region.Prefix, rand.Intn(255), rand.Intn(255))
	return ip, region.Name
}

// genTargetUrl 生成目标 Url
func genTargetUrl(baseUrl string) string {
	var clientId string
	if rand.Float32() < 0.1 {
		baseId := int64(880000000000)
		clientId = fmt.Sprintf("%012d", baseId+rand.Int63n(10000))
	} else {
		baseId := int64(990000000000)
		clientId = fmt.Sprintf("%012d", baseId+rand.Int63n(10000))
	}

	return fmt.Sprintf("%s/api/orders/%s?fmt=j", baseUrl, clientId)
}

func worker(ctx context.Context, client *http.Client, baseUrl string, jobs <-chan setVal, spoofIp bool) {
	for range jobs {
		url := genTargetUrl(baseUrl)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)

		var regionName string
		if spoofIp {
			ip, region := getSimPair()
			req.Header.Set("X-Real-IP", ip)
			req.Header.Set("X-Forwarded-For", ip)
			regionName = region
		}

		st := time.Now()
		resp, err := client.Do(req)
		t := time.Since(st).Seconds()

		status, cacheRst := "0", "UNKNOWN_ERR"
		if err == nil {
			status = strconv.Itoa(resp.StatusCode)
			cacheRst = resp.Header.Get("X-Cache-Status")
			if cacheRst == "" {

				switch resp.StatusCode {
				case 429:
					// 应用层 429 限流
					cacheRst = "RATE_LIMITED"
				case 503:
					// Nginx 接入层限流
					cacheRst = "NGINX_LIMITED"
				default:
					cacheRst = "MISSING_HEADER"
				}
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

		clientRequestsTotal.WithLabelValues(status, cacheRst, regionName).Inc()
		clientRequestDuration.WithLabelValues(status, cacheRst, regionName).Observe(t)
	}
}

func main() {
	rateParam := flag.Int("rate", 5000, "QPS/每秒请求数")
	timeParam := flag.Duration("time", 30*time.Minute, "压测持续时间")
	tarUrl := flag.String("url", "http://localhost", "Nginx 网关地址")
	spoofIp := flag.Bool("spoof-ip", true, "是否伪造随机 ip")
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
		MaxIdleConns:        c,
		MaxIdleConnsPerHost: 8000,
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
		go worker(ctx, client, *tarUrl, jobs, *spoofIp)
	}

	limiter := rate.NewLimiter(rate.Limit(*rateParam), max(*rateParam/10, 1))

Loop:
	for {
		if err := limiter.Wait(ctx); err != nil {
			fmt.Printf("到达设定时限 %v 或被中断\n", *timeParam)
			break Loop
		}

		select {
		case jobs <- setVal{}:
		default:
			log.Println("Warning! Worker pool 满载, 请求堆积...")
		}
	}

	time.Sleep(3 * time.Second)
	fmt.Println("Done.")
}
