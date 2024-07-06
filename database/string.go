package database

import (
	databaseface "redis/interface/database"
	"redis/interface/resp"
	"redis/lib/utils"
	"redis/resp/reply"
)

// GET K1
func execGet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeBulkReply(bytes)
}

// SET K1 v
func execSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &databaseface.DataEntity{Data: value}
	db.PutEntity(key, entity)
	db.addAof(utils.ToCmdLine2("SET", args...))
	return reply.MakeOkReply()
}

// SETNX K1 v
func execSetNX(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity := &databaseface.DataEntity{Data: value}
	result := db.PutIfAbsent(key, entity)
	db.addAof(utils.ToCmdLine2("SETNX", args...))
	return reply.MakeIntReply(int64(result))
}

// GETSET K1 v1
func execGetSet(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	value := args[1]
	entity, exists := db.GetEntity(key)
	db.PutEntity(key, &databaseface.DataEntity{Data: value})
	db.addAof(utils.ToCmdLine2("GETSET", args...))
	if !exists {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeBulkReply(entity.Data.([]byte))
}

// STRLEN K
func execStrLen(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	bytes := entity.Data.([]byte)
	return reply.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("GET", execGet, 2)
	RegisterCommand("SET", execSet, 3)
	RegisterCommand("SETNX", execSetNX, 3)
	RegisterCommand("GETSET", execGetSet, 3)
	RegisterCommand("STRLEN", execStrLen, 2)
}
