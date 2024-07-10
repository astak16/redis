package database

import (
	databaseface "redis/interface/database"
	"redis/interface/resp"
	"redis/lib/utils"
	"redis/resp/reply"
)

// GET K1
func GET(db *DB, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 K1
	key := string(args[0])
	// 通过 K1 获取 entity
	entity, exists := db.GetEntity(key)
	// 如果 K1 不存在，返回 nil
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	// 将 data 包装成 MakeBulkReply
	return reply.MakeBulkReply(bytes)
}

// SET K1 v
func SET(db *DB, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 K1
	key := string(args[0])
	// 从用户输入的命令中拿到 v
	value := args[1]
	// 将 v 存入 entity
	entity := &databaseface.DataEntity{Data: value}
	// 将 K1 和 entity 存入 db
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine2("SET", args...))
	// 返回 OK
	return reply.MakeOkReply()
}

// SETNX K1 v
func SETNX(db *DB, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 K1
	key := string(args[0])
	// 从用户输入的命令中拿到 v
	value := args[1]
	// 将 v 存入 entity
	entity := &databaseface.DataEntity{Data: value}
	// 调用 PutIfAbsent，将 K1 和 entity 存入 db，如果 key 不存在，返回 1，存在返回 0
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine2("SETNX", args...))
	return reply.MakeIntReply(int64(result))
}

// GETSET K1 v1
func GETSET(db *DB, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 K1
	key := string(args[0])
	// 从用户输入的命令中拿到 v1
	value := args[1]
	// 通过 K1 获取 entity
	entity, exists := db.GetEntity(key)
	// 将 v1 转为 []byte 存入 db 中
	db.PutEntity(key, &databaseface.DataEntity{Data: value})
	db.addAof(utils.ToCmdLine2("GETSET", args...))
	// 如果 K1 不存在，返回 nil
	if !exists {
		return reply.MakeNullBulkReply()
	}
	// 返回 K1 的旧值
	return reply.MakeBulkReply(entity.Data.([]byte))
}

// STRLEN K
func STRLEN(db *DB, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 K
	key := string(args[0])
	// 通过 K 获取 entity
	entity, exists := db.GetEntity(key)
	// 如果 K 不存在，返回 null
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	// 返回 K 的长度
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("GET", GET, 2)
	RegisterCommand("SET", SET, 3)
	RegisterCommand("SETNX", SETNX, 3)
	RegisterCommand("GETSET", GETSET, 3)
	RegisterCommand("STRLEN", STRLEN, 2)
}
