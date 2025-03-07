package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

type ConsistentHash struct {
	sync.RWMutex
	hashFunc func(data []byte) uint32
	replicas int
	ring     map[uint32]string   // 虚拟节点哈希到物理节点
	nodeMap  map[string][]uint32 // 物理节点到虚拟节点哈希列表
	keys     []uint32            // 有序的虚拟节点哈希
}

func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		hashFunc: func(data []byte) uint32 {
			h := fnv.New32a()
			h.Write(data)
			return h.Sum32()
		},
		replicas: replicas,
		ring:     make(map[uint32]string),
		nodeMap:  make(map[string][]uint32),
		keys:     make([]uint32, 0),
	}
}

func (c *ConsistentHash) AddNode(node string) {
	c.Lock()
	defer c.Unlock()

	if _, exists := c.nodeMap[node]; exists {
		return
	}

	hashes := make([]uint32, 0, c.replicas)
	for i := 0; i < c.replicas; i++ {
		virtualNode := fmt.Sprintf("%s#%d", node, i)
		hash := c.hashFunc([]byte(virtualNode))

		if _, exists := c.ring[hash]; !exists {
			c.ring[hash] = node
			hashes = append(hashes, hash)
			c.keys = append(c.keys, hash)
		}
	}

	c.nodeMap[node] = hashes
	sort.Slice(c.keys, func(i, j int) bool {
		return c.keys[i] < c.keys[j]
	})
}

func (c *ConsistentHash) RemoveNode(node string) {
	c.Lock()
	defer c.Unlock()

	hashes, exists := c.nodeMap[node]
	if !exists {
		return
	}

	for _, hash := range hashes {
		delete(c.ring, hash)
	}
	delete(c.nodeMap, node)

	// 重建哈希环
	c.keys = c.keys[:0]
	for hash := range c.ring {
		c.keys = append(c.keys, hash)
	}
	sort.Slice(c.keys, func(i, j int) bool {
		return c.keys[i] < c.keys[j]
	})
}

func (c *ConsistentHash) GetNode(key string) (string, bool) {
	c.RLock()
	defer c.RUnlock()

	if len(c.keys) == 0 {
		return "", false
	}

	hash := c.hashFunc([]byte(key))
	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= hash
	})

	if idx == len(c.keys) {
		idx = 0
	}

	if node, ok := c.ring[c.keys[idx]]; ok {
		return node, true
	}
	return "", false
}

func main() {
	ch := NewConsistentHash(3)

	// 添加节点
	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// 查询节点
	if node, ok := ch.GetNode("user123"); ok {
		fmt.Println("Key 'user123' belongs to node:", node)
	}

	// 删除节点
	ch.RemoveNode("node2")

	// 再次查询
	if node, ok := ch.GetNode("user123"); ok {
		fmt.Println("After removal, key 'user123' belongs to node:", node)
	}
}
