package handler

import (
	"context"
	"errors"
	"io"
	"net"
	"redis/cluster"
	"redis/config"
	"redis/database"
	databaseface "redis/interface/database"
	"redis/lib/logger"
	"redis/lib/sync/atomic"
	"redis/resp/connection"
	"redis/resp/parser"
	"redis/resp/reply"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type RespHandler struct {
	activeConn sync.Map              // 记录用户的连接
	db         databaseface.Database // 核心数据库，操作 kev value 相关的逻辑
	closing    atomic.Boolean        // 是否关闭
}

func MakeHandler() *RespHandler {
	var db databaseface.Database
	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewStandaloneDatabase()
	}
	return &RespHandler{db: db}
}

// 关闭一个客户端
func (r *RespHandler) closeClient(client *connection.Connection) {
	// 关闭客户端
	_ = client.Close()
	// 关闭客户端后的善后工作
	r.db.AfterClientClose(client)
	// 从 sync.Map 中删除掉这个客户端
	r.activeConn.Delete(client)
}

// 处理用户的指令
func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	//是不是在关闭中
	if r.closing.Get() {
		_ = conn.Close()
	}
	// 新建连接
	client := connection.NewConn(conn)
	// 将连接存储到 sync.Map 中
	r.activeConn.Store(client, struct{}{})
	// 调用 ParseStream 函数，解析用户的指令
	// 通过 chan 拿到解析后的结果
	ch := parser.ParseStream(conn)
	for payload := range ch {
		// 处理 payload 中的错误
		if payload.Err != nil {
			// io 错误
			// io.EOF 表示已经解析到结尾了，连接可以正常关闭了
			// io.ErrUnexpectedEOF 表示在预期的数据结束之前遇到了 EOF，可能发生了数据传输突然中断了
			// use of close network connection 表示使用已经关闭的连接，这个时候也需要关闭连接
			// 只要出现这三种情况，就需要关闭连接
			if payload.Err == io.EOF || errors.Is(payload.Err, io.ErrUnexpectedEOF) || strings.Contains(payload.Err.Error(), "use of close network connection") {
				r.closeClient(client)
				logger.Infof("connection closed: " + client.RemoteAddr().String())
				return
			}
			// 解析 resp 协议出错，MakeErrReply 作用是将错误信息前面加上 -ERR，后面加上 \r\n
			errReply := reply.MakeErrReply(payload.Err.Error())
			// 将解析 resp 协议出错的信息返回给用户
			err := client.Write(errReply.ToBytes())
			// 给用户写入数据可能会出错，如果出错，关闭连接
			if err != nil {
				r.closeClient(client)
				logger.Infof("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		// 没有数据
		if payload.Data == nil {
			continue
		}
		replyResult, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Infof("require multi bulk reply")
			continue
		}
		// 调用数据库的 Exec 方法，执行用户的指令
		result := r.db.Exec(client, replyResult.Args)
		// 将执行结果返回给用户
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			// 如果执行结果为空，返回一个未知错误
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// 关闭所有的客户端
func (r *RespHandler) Close() error {
	logger.Info("handler shutting down")
	// 将关闭连接的标志设置为 true
	r.closing.Set(true)
	// 使用 Range 遍历 sync.Map
	// 接收一个匿名函数，在这个函数中做关闭连接的操作，并且最后需要返回 true，表示继续遍历
	r.activeConn.Range(func(key, value any) bool {
		// 需要将 key 断言成 *connection.Connection 类型
		client := key.(*connection.Connection)
		// 关闭连接
		_ = client.Close()
		// 返回 true，表示继续遍历，返回 false，表示停止遍历
		return true
	})
	// 关闭数据库
	r.db.Close()
	return nil
}
