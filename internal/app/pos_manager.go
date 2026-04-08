package app

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	mysqlorg "github.com/go-mysql-org/go-mysql/mysql"
)

// LogEntry 兼容传统与 GTID 模式
type LogEntry struct {
	Posi mysqlorg.Position `json:"posi"`
	GTID string            `json:"gtid,omitempty"`
}

// PosManager 负责 Binlog Position 的持久化管理
type PosManager struct {
	filePath string

	// 存储 LogEntry {mysqlorg.Position, Gtid string}
	cur atomic.Value
	lst atomic.Value

	mu      sync.Mutex  // Flush中, 防止并发写
	stopped atomic.Bool // 停止标记
}

const (
	fIntrvl   = 5 * time.Second
	tmpSuffix = ".tmp"
)

func NewPosManager(dataDir string) *PosManager {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("[Pos] Warning! Cannot make dir %s: %v", dataDir, err)
	}
	p := &PosManager{filePath: filepath.Join(dataDir, "parse.dat")} // 仿 Java 命名

	// 预装填空点位
	emptyle := LogEntry{Posi: mysqlorg.Position{}}
	p.cur.Store(emptyle)
	p.lst.Store(emptyle)
	if err := p.cleanTmp(); err != nil {
		log.Printf("[Pos] Warning! Cannot cleanup tmpfile: %v", err)
	}

	go p.flushLoop()
	return p
}

// Save 更新内存中的位点快照
func (p *PosManager) Save(pos mysqlorg.Position, gtidSet mysqlorg.GTIDSet) {
	if p.stopped.Load() {
		return
	}

	le := LogEntry{Posi: pos}
	if gtidSet != nil {
		le.GTID = gtidSet.String() // To string 存储
	}
	p.cur.Store(le)
}

// Load 从磁盘加载, 用于断点续传；仅在程序启动时调用 1 次 [磁盘记忆 -> 内存]
func (p *PosManager) Load() error {
	data, err := os.ReadFile(p.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("[Pos] Historical pos not found, new start..")
			return nil // 不存在 -> 初次启动
		}
		return fmt.Errorf("[Pos] Read failed: %w", err)
	}
	if len(data) == 0 {
		log.Println("[Pos] Warning! parse.dat is empty")
		return nil
	}

	var le LogEntry
	if err := json.Unmarshal(data, &le); err != nil {
		return fmt.Errorf("[Pos] Unmarshal .json failed: %w", err)
	}
	p.lst.Store(le)
	p.cur.Store(le)

	return nil
}

// Get 无锁返回内存中最新的位点
func (p *PosManager) Get() LogEntry {
	return p.cur.Load().(LogEntry)
}

// Flush 执行实际的原子磁盘写入操作, 由后台 goroutine 定期调用
func (p *PosManager) Flush() error {
	curEntry, lstEntry := p.cur.Load().(LogEntry), p.lst.Load().(LogEntry)
	// 跳过无效点位
	if curEntry.Posi.Name == "" || curEntry.Posi.Pos == 0 {
		return nil
	}
	// 跳过未变化的点位
	if curEntry == lstEntry {
		return nil
	}
	// 写磁盘, lock
	p.mu.Lock()
	defer p.mu.Unlock()

	lstEntry = p.lst.Load().(LogEntry)
	if curEntry == lstEntry {
		return nil // 双检查锁
	}

	tmpFile := p.filePath + tmpSuffix
	data, err := json.Marshal(curEntry)
	if err != nil {
		return fmt.Errorf("Marshal .json failed: %w", err)
	}
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("Open/create tmp: %w", err)
	}
	// defer f.Close()

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("Write failed: %w", err)
	}

	// 强制刷盘, 达到物理同步
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("Sync err: %w", err)
	}

	// 显式关闭
	if err := f.Close(); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("Close err: %w", err)
	}

	if err := os.Rename(tmpFile, p.filePath); err != nil {
		return fmt.Errorf("Rename err: %w", err)
	}

	p.lst.Store(curEntry)
	log.Printf("[Pos] Flush done: {%s, %d}, %s", curEntry.Posi.Name, curEntry.Posi.Pos, curEntry.GTID)
	return nil
}

// flushLoop 后台定期 flush goroutine
func (p *PosManager) flushLoop() {
	tk := time.NewTicker(fIntrvl)
	defer tk.Stop()
	for range tk.C {
		if p.stopped.Load() {
			return
		}
		if err := p.Flush(); err != nil {
			log.Printf("[Pos] Warning! An exception occurred during flushLoop, %v", err)
		}
	}
}

// cleanTmp 清理上次崩溃留下的临时文件
func (p *PosManager) cleanTmp() error {
	tmpFile := p.filePath + tmpSuffix
	if _, err := os.Stat(tmpFile); err != nil {
		log.Printf("[Pos] Tmpfile found, cleaning: %s", tmpFile)
		if err := os.Remove(tmpFile); err != nil {
			return err
		}
	}
	return nil
}

func (p *PosManager) Close() {
	log.Println("[Pos] Closing pos_manager..")
	p.stopped.Store(true)
	if err := p.Flush(); err != nil {
		log.Printf("[Pos] Final flush failed: %v", err)
	} else {
		log.Println("[Pos] Closing done")
	}
}
