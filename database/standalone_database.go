package database

import (
	"redis/aof"
	"redis/config"
	"redis/interface/resp"
	"redis/lib/logger"
	"redis/resp/reply"
	"strconv"
	"strings"
)

type StandaloneDatabase struct {
	dbSet      []*DB
	aofHandler *aof.AofHandler // 增加落盘功能
}

func NewStandaloneDatabase() *StandaloneDatabase {
	database := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// 初始化 16 个数据库
	database.dbSet = make([]*DB, config.Properties.Databases)
	// 初始化每一个数据库
	for i := range database.dbSet {
		db := makeDB()
		db.index = i
		database.dbSet[i] = db
	}
	// 先看下配置文件中的 appendonly 是否为 true
	if config.Properties.AppendOnly {
		// 初始化 aofHandler
		aofHandler, err := aof.NewAofHandler(database)
		if err != nil {
			panic(err)
		}
		// 持有 aofHandler
		database.aofHandler = aofHandler
		// 遍历 dbSet
		for _, db := range database.dbSet {
			// 解决闭包问题
			sdb := db
			// 为每个 db 添加 AddAof 方法
			// 这个 addAof 方法是在执行指令的时候调用的
			sdb.addAof = func(line CmdLine) {
				database.aofHandler.AddAof(sdb.index, line)
			}
		}
	}
	return database
}

func (database *StandaloneDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	// 用来 recover panic
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	// 拿到第一个参数，也就是指令的名字
	cmdName := strings.ToLower(string(args[0]))
	// 如果指令是 select，切换数据库
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		execSelect(client, database, args[1:])
	}
	// 从 client 中拿到 dbIndex
	dbIndex := client.GetDBIndex()
	// 通过 dbIndex 拿到 db
	db := database.dbSet[dbIndex]
	// 执行指令
	return db.Exec(client, args)
}

func (database *StandaloneDatabase) Close() {
}

func (database *StandaloneDatabase) AfterClientClose(c resp.Connection) {
}

// select 2
func execSelect(c resp.Connection, database *StandaloneDatabase, args [][]byte) resp.Reply {
	// 从用户输入的命令中拿到 dbIndex
	dbIndex, err := strconv.Atoi(string(args[0]))
	// 如果 dbIndex 不是数字，返回错误
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	// 如果 dbIndex 超出了数据库的范围，返回错误
	if dbIndex >= len(database.dbSet) {
		return reply.MakeErrReply("ERR db index is out of range")
	}
	// 设置 dbIndex
	c.SelectDB(dbIndex)
	// 返回 OK
	return reply.MakeOkReply()
}
