package database

import (
	"fmt"
	"redis/interface/resp"
	"redis/lib/utils"
	"redis/lib/wildcard"
	"redis/resp/reply"
)

// DEL K1 K2 K3
func DEL(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("DEL", args...))
	}
	return reply.MakeIntReply(int64(deleted))
}

// EXISTS K1 K2 K3 ...
func EXISTS(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// FLUSHDB
func FLUSHDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine2("FLUSEHDB", args...))
	return reply.MakeOkReply()
}

// TYPE K1
func TYPE(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	// 如果 key 不存在，返回 none
	if !exists {
		return reply.MakeStatusReply("none") // TCP :none\r\n
	}
	switch entity.Data.(type) {
	// 如果是 []byte 类型，返回 string
	case []byte:
		return reply.MakeStatusReply("string")
	}
	return reply.MakeUnknownErrReply()
}

// RENAMENX K1 K2
func RENAMENX(db *DB, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 K1
	src := string(args[0])
	// 从用户输入的命令中拿到 K2
	dest := string(args[1])
	_, ok := db.GetEntity(dest)
	// 如果 K2 存在，啥也不改
	if ok {
		return reply.MakeIntReply(0)
	}
	entity, exists := db.GetEntity(src)
	// 如果 K1 不存在，返回错误
	if !exists {
		return reply.MakeErrReply("no such key")
	}
	// 将 K2 的值设置为 K1 的 entity
	db.PutEntity(dest, entity)
	// 删除 K1
	db.Remove(src)
	db.addAof(utils.ToCmdLine2("RENAMENX", args...))
	return reply.MakeIntReply(1)
}

// RENAME K1 K2
func RENAME(db *DB, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 K1
	src := string(args[0])
	// 从用户输入的命令中拿到 K2
	dest := string(args[1])
	// 通过 K1 获取 entity
	entity, exists := db.GetEntity(src)
	// 如果 K1 不存在，返回错误
	if !exists {
		return reply.MakeErrReply("no such key")
	}
	// 将 K2 的值设置为 K1 的 entity
	db.PutEntity(dest, entity)
	// 删除 K1
	db.Remove(src)
	db.addAof(utils.ToCmdLine2("RENAME", args...))
	return reply.MakeOkReply()
}

// KEYS *
func KEYS(db *DB, args [][]byte) resp.Reply {
	// 通过 wildcard 包来实现通配符
	pattern := wildcard.CompilePattern(string(args[0]))
	fmt.Println(pattern, "sssssss")
	// 存放所有通配符的 key
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		// 如果 key 匹配通配符，就存入 result
		if pattern.IsMatch(key) {
			// 将 key 转为 []byte 类型，存入 result
			result = append(result, []byte(key))
		}
		// 返回 true，继续遍历
		return true
	})
	// 返回所有匹配的 key
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("DEL", DEL, -2)
	RegisterCommand("EXISTS", EXISTS, -2)
	RegisterCommand("FLUSHDB", FLUSHDB, -1)  // FLUSHDB a b c，-1 的意思是忽略 a b c，只拿到 FLUSHDB
	RegisterCommand("TYPE", TYPE, 2)         // TYPE K1
	RegisterCommand("RENAME", RENAME, 3)     // RENAME K1 K2
	RegisterCommand("RENAMENX", RENAMENX, 3) // RENAMENX K1 K2
	RegisterCommand("KEYS", KEYS, 2)         // KEYS *
}
