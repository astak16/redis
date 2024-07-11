package database

import (
	"redis/datastruct/dict"
	databaseface "redis/interface/database"
	"redis/interface/resp"
	"redis/resp/reply"
	"strings"
)

type DB struct {
	index  int           // 数据的编号
	data   dict.Dict     // 数据类型
	addAof func(CmdLine) // 每个 db 都有一个 addAof 方法
}

type ExecFunc func(db *DB, args [][]byte) resp.Reply

type CmdLine = [][]byte

func makeDB() *DB {
	return &DB{
		data:   dict.MakeSyncDict(),
		addAof: func(line CmdLine) {},
	}
}

func (db *DB) Exec(c resp.Connection, cmdLine CmdLine) resp.Reply {
	// 拿到指令的名字，比如 GET、SET
	cmdName := strings.ToLower(string(cmdLine[0]))
	// 从 cmdTable 中拿到指令的结构体
	cmd, ok := cmdTable[cmdName]
	// 如果指令不存在，返回错误
	if !ok {
		return reply.MakeErrReply("ERR unknown command " + cmdName)
	}
	// 验证参数的数量，如果参数数量不对，返回错误
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	// 拿到指令的执行方法
	fun := cmd.exector
	// 执行指令，传入 db 和指令的参数
	// SET key value，cmdLine[1:] 就是 key 和 value
	return fun(db, cmdLine[1:])
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	// 固定参数，比如 SET Key Value，所以参数数量必须等于 arity
	if arity > 0 {
		return argNum == arity
	}
	// 不固定参数，比如 EXISTS k1 k2 ...，这种参数不固定的，arity 会赋值 -2，所以参数数量必须大于等于 -arity
	return argNum >= -arity
}

// 下面这些方法是 db 的方法，是对 dict 的封装
func (db *DB) GetEntity(key string) (*databaseface.DataEntity, bool) {
	// 调用 dict 的 Get 方法，通过 key 获取 value
	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	// 底层 dict 存的是 interface{}，所以需要转为 *DataEntity
	entity, _ := raw.(*databaseface.DataEntity)
	return entity, true
}

func (db *DB) PutEntity(key string, entity *databaseface.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DB) PutIfExists(key string, entity *databaseface.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DB) PutIfAbsent(key string, entity *databaseface.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	// 遍历 keys，删除每一个 key
	for _, key := range keys {
		// 先判断 key 是否存在，存在就删除，并且 deleted++
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	// 返回删除的数量
	return deleted
}

func (db *DB) Flush() {
	db.data.Clear()
}
