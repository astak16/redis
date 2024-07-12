package consistenthash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc    HashFunc       // hash 函数
	nodeHashs   []int          // 节点 hash 值的切片，按顺序保存
	nodehashMap map[int]string // 节点 hash 值和节点名称的映射
}

// hash 函数可以自定义
func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodehashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		// 默认使用 crc32.ChecksumIEEE 函数
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

func (m *NodeMap) AddNode(keys ...string) {
	// 遍历节点切片，计算节点的 hash 值
	for _, key := range keys {
		if key == "" {
			continue
		}
		// 计算节点的 hash 值
		hash := int(m.hashFunc([]byte(key)))
		// 保存节点的 hash 值
		m.nodeHashs = append(m.nodeHashs, hash)
		// 保存节点的 hash 值和节点名称的映射
		m.nodehashMap[hash] = key
	}
	// 对节点的 hash 值排序
	sort.Ints(m.nodeHashs)
}

// 传入一个 key，返回这个 key 应该去的节点
func (m *NodeMap) PickNode(key string) string {
	// 如果节点为空，返回空字符串
	if m.IsEmpty() {
		return ""
	}
	// 计算 key 的 hash 值
	hash := int(m.hashFunc([]byte(key)))
	// 在 nodeHashs 中找到第一个大于 hash 的节点
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})
	// 如果没有找到，说明 key 的 hash 值比所有节点的 hash 值都大，那么就返回第一个节点
	if idx == len(m.nodeHashs) {
		idx = 0
	}
	// 返回节点名称
	return m.nodehashMap[m.nodeHashs[idx]]
}
