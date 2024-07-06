package databaseface

import "redis/interface/resp"

type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply // 执行指令：执行的客户端 client，执行的指令 args，返回的是 Reply
	Close()                                                // 关闭数据库
	AfterClientClose(c resp.Connection)                    // 数据库关闭之后的善后工作
}

type DataEntity struct {
	Data interface{}
}
