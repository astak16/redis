package dict

import "sync"

type SyncDict struct {
	m sync.Map
}

func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	//sync.Map 获取值的方法是 Load
	val, ok := dict.m.Load(key)
	return val, ok
}

func (dict *SyncDict) Len() int {
	length := 0
	dict.m.Range(func(key, value any) bool {
		length++
		return true
	})
	return length
}

func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	// 先设置
	dict.m.Store(key, val)
	// 插入失败，返回 0
	if existed {
		return 0
	}
	// 插入成功，返回 1
	return 1
}

func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	// 插入失败，返回 0
	if existed {
		return 0
	}
	// 后设置
	dict.m.Store(key, val)
	// 插入成功，返回 1
	return 1
}

func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		// 存在才能设置
		dict.m.Store(key, val)
		// 存在返回 1
		return 1
	}
	// 不存在返回 0
	return 0
}

func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key)
	// 先删除
	dict.m.Delete(key)
	if existed {
		// 如果 key 存在返回 1
		return 1
	}
	// 如果 key 不存在返回 0
	return 0
}

func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value any) bool {
		consumer(key.(string), value)
		return true
	})
}

func (dict *SyncDict) Keys() []string {
	// 用 dict.Len() 设置切片的长度
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value any) bool {
		// 遍历时，将 key 转为 string 类型，存入切片
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

func (dict *SyncDict) RandomKeys(limit int) []string {
	// 用 limit 设置切片的长度
	result := make([]string, dict.Len())
	// 遍历 limit
	for i := 0; i < limit; i++ {
		// 通过随机数获取 key
		dict.m.Range(func(key, value any) bool {
			result[i] = key.(string)
			// return false 的作用是只遍历一次，这样就保证一次 for 循环时，随机获取其中一个 key
			return false
		})
	}
	return result
}

func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value any) bool {
		result[i] = key.(string)
		i++
		// 如果 i 等于 limit 说明遍历了 limit 次，就返回 false，结束遍历
		if i == limit {
			return false
		}
		return true
	})
	return result
}

func (dict *SyncDict) Clear() {
	*dict = *MakeSyncDict()
}
