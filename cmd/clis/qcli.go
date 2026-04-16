package main

import (
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// NewQsysTargeter 动态构造压测 requests
// 70% 有单, 30% 无单 (测试 redis 防穿透)
func NewQsysTargeter(baseUrl string) vegeta.Targeter {
	return func(tar *vegeta.Target) error {
		if tar == nil {
			return vegeta.ErrNilTarget
		}
		tar.Method = http.MethodGet

		var clientId string
		if rand.Float32() < 0.7 {
			// 70% 对应 qsim 生成的活跃号段
			baseId := int64(880000000000)
			clientId = fmt.Sprintf("%012d", baseId+rand.Int63n(10000))
		} else {
			baseId := int64(990000000000)
			clientId = fmt.Sprintf("%012d", baseId+rand.Int63n(10000))
		}

		tar.URL = fmt.Sprintf("%s/api/orders/%s?fmt=j", baseUrl, clientId)
		return nil
	}
}

func main() {
	rateParam := flag.Int("rate", 5000, "QPS/每秒请求数")
	timeParam := flag.Duration("time", 10*time.Second, "压测持续时间")
	tarUrl := flag.String("url", "http://localhost", "Nginx 网关地址")
	flag.Parse()
	fmt.Println("Qsys http 压测启动..")

	// 配置 Vetega attacker
	rate := vegeta.Rate{Freq: *rateParam, Per: time.Second}
	t, targeter := *timeParam, NewQsysTargeter(*tarUrl)

	// 设置发包器
	attacker := vegeta.NewAttacker(
		vegeta.Timeout(6*time.Second), // 对应 Fiber rdTout/wtTout
		vegeta.KeepAlive(true),
	)

	var metrics vegeta.Metrics
	for rst := range attacker.Attack(targeter, rate, t, "Qsys-query-test") {
		metrics.Add(rst)
	}
	metrics.Close()

	rpt := vegeta.NewTextReporter(&metrics)
	fmt.Println("压测结果: ")
	rpt.Report(os.Stdout)
}
