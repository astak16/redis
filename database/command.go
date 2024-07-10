package database

import "strings"

var cmdTable = make(map[string]*command)

type command struct {
	exector ExecFunc // 执行方法
	arity   int      // 参数数量
}

func RegisterCommand(name string, exector ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{exector: exector, arity: arity}
}
