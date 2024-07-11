package aof

import (
	"io"
	"os"
	"redis/config"
	databaseface "redis/interface/database"
	"redis/lib/logger"
	"redis/lib/utils"
	"redis/resp/connection"
	"redis/resp/parser"
	"redis/resp/reply"
	"strconv"
)

const aofBufferSize = 1 << 16

type CmdLine = [][]byte
type payload struct {
	cmdLine CmdLine // 指令
	dbIndex int     // db 索引
}

type AofHandler struct {
	database    databaseface.Database // 持有 db，db 有业务核心
	aofFile     *os.File              // 持有 aof 文件
	aofFilename string                // aof 文件名
	currentDB   int                   // 当前 db
	aofChan     chan *payload         // 写文件的缓冲区
}

func NewAofHandler(database databaseface.Database) (*AofHandler, error) {
	// 初始化 AofHandler 结构体
	handler := &AofHandler{}
	// 从配置文件中读取 aof 文件名
	handler.aofFilename = config.Properties.AppendFilename
	// 持有 db
	handler.database = database
	// 从硬盘加载 aof 文件
	handler.LoadAof()
	// 打开 aof 文件, 如果不存在则创建
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	// 持有 aof 文件
	handler.aofFile = aofFile
	// 初始化 aofChan
	handler.aofChan = make(chan *payload, aofBufferSize)
	// 启动一个 goroutine 处理 aofChan
	go func() {
		handler.HandleAof()
	}()
	// 返回 AofHandler 结构体
	return handler, nil
}

func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	// 如果配置文件中的 appendonly 为 true 并且 aofChan 不为 nil
	if config.Properties.AppendOnly && handler.aofChan != nil {
		// 往 aofChan 中添加指令
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

func (handler *AofHandler) HandleAof() {
	// 初始化 currentDB
	handler.currentDB = 0
	// 遍历 chan
	for p := range handler.aofChan {
		// 如果当前 db 不等于上一次工作的 db，就要插入一条 select 语句
		if p.dbIndex != handler.currentDB {
			// 我们要把 select 0 编码成 RESP 格式
			// 也就是 *2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			// 写入 aof 文件
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue
			}
			// 更新 currentDB
			handler.currentDB = p.dbIndex
		}
		// 这里是插入正常的指令
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		// 写入 aof 文件
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
	}
}

func (handler *AofHandler) LoadAof() {
	// 打开 aof 文件
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	// 关闭文件
	defer func() {
		_ = file.Close()
	}()
	// 创建一个 RESP 解析器，将 file 传入，解析后的指令会放到 chan 中
	ch := parser.ParseStream(file)
	fackConn := &connection.Connection{}
	// 遍历 chan，执行指令
	for p := range ch {
		if p.Err != nil {
			// 如果是 EOF，说明文件读取完毕
			if p.Err == io.EOF {
				break
			}
			logger.Error(err)
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		// 将指令转换成 MultiBulkReply 类型
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("exec multi mulk")
			continue
		}
		// 执行指令
		rep := handler.database.Exec(fackConn, r.Args)
		if reply.IsErrReply(rep) {
			logger.Error(rep)
		}
	}
}
