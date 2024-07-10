package database

import (
	"redis/interface/resp"
	"redis/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

func init() {
	RegisterCommand("ping", Ping, 1) // 注册 Ping 指令，参数数量 arity 是 1
}
