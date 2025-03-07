package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"time"
)

// BackendServer 后端服务器结构
type BackendServer struct {
	URL     string
	Healthy bool
	mu      sync.RWMutex
}

// LoadBalancer 负载均衡器
type LoadBalancer struct {
	backends      []*BackendServer
	currentIndex  uint64
	mu            sync.RWMutex
	healthCheckCh chan struct{}
	interval      time.Duration
}

// NewLoadBalancer 创建负载均衡器
func NewLoadBalancer(interval time.Duration) *LoadBalancer {
	return &LoadBalancer{
		healthCheckCh: make(chan struct{}),
		interval:      interval,
	}
}

// 轮询选择后端服务器
func (lb *LoadBalancer) getNextBackend() (*BackendServer, error) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if len(lb.backends) == 0 {
		return nil, errors.New("no available backends")
	}

	start := atomic.AddUint64(&lb.currentIndex, 1)
	size := uint64(len(lb.backends))

	for i := start; i < start+size; i++ {
		index := i % size
		backend := lb.backends[index]

		backend.mu.RLock()
		healthy := backend.Healthy
		backend.mu.RUnlock()

		if healthy {
			if atomic.CompareAndSwapUint64(&lb.currentIndex, start, i) {
				return backend, nil
			}
		}
	}
	return nil, errors.New("no healthy backends")
}

// AddBackend 添加后端服务器
func (lb *LoadBalancer) AddBackend(url string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	for _, b := range lb.backends {
		if b.URL == url {
			return
		}
	}

	lb.backends = append(lb.backends, &BackendServer{
		URL:     url,
		Healthy: true,
	})
}

// RemoveBackend 移除后端服务器
func (lb *LoadBalancer) RemoveBackend(url string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	for i, b := range lb.backends {
		if b.URL == url {
			lb.backends = append(lb.backends[:i], lb.backends[i+1:]...)
			return
		}
	}
}

// 健康检查
func (lb *LoadBalancer) checkHealth() {
	client := http.Client{Timeout: 2 * time.Second}

	for _, backend := range lb.backends {
		go func(b *BackendServer) {
			resp, err := client.Head(b.URL)
			b.mu.Lock()
			defer b.mu.Unlock()

			if err != nil || resp.StatusCode >= 500 {
				b.Healthy = false
			} else {
				b.Healthy = true
			}
		}(backend)
	}
}

// 启动健康检查
func (lb *LoadBalancer) StartHealthCheck() {
	ticker := time.NewTicker(lb.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lb.checkHealth()
		case <-lb.healthCheckCh:
			return
		}
	}
}

// 停止健康检查
func (lb *LoadBalancer) StopHealthCheck() {
	close(lb.healthCheckCh)
}

/****************** 测试用例 ​******************/
func main() {
	// 创建测试HTTP服务器
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts2.Close()

	// 初始化负载均衡器
	lb := NewLoadBalancer(5 * time.Second)

	// 添加后端服务器
	lb.AddBackend(ts1.URL)
	lb.AddBackend(ts2.URL)

	// 启动健康检查
	go lb.StartHealthCheck()
	defer lb.StopHealthCheck()

	// 模拟请求分发
	for i := 0; i < 10; i++ {
		go func(n int) {
			if backend, err := lb.getNextBackend(); err == nil {
				fmt.Printf("Request %d -> %s\n", n, backend.URL)
			}
		}(i)
	}

	// 等待健康检查生效
	time.Sleep(6 * time.Second)

	// 测试移除节点
	lb.RemoveBackend(ts2.URL)
	fmt.Println("\nAfter remove unhealthy backend:")
	for i := 0; i < 3; i++ {
		if backend, err := lb.getNextBackend(); err == nil {
			fmt.Printf("Request %d -> %s\n", i, backend.URL)
		}
	}
}
