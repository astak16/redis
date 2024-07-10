package dict

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Put(key string, val interface{}) (result int)         // 存进去回复 1，没存进去回复 0
	PutIfAbsent(key string, val interface{}) (result int) // 如果 key 不存在，才能设置，如果 key 存在回复 0，不存在回复 1
	PutIfExists(key string, val interface{}) (result int) // 如果 key 存在，才能设置，如果 key 存在回复 1，不存在回复 0
	Remove(key string) (result int)
	ForEach(consumer Consumer)             // 遍历所有键值对
	Keys() []string                        // 获取所有的 key
	RandomKeys(limit int) []string         // 返回 n 个 key
	RandomDistinctKeys(limit int) []string // 返回 n 个不重复的 key
	Len() int
	Clear() // 清空数据结构
}
