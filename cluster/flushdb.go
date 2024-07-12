package cluster

import (
	"redis/interface/resp"
	"redis/resp/reply"
)

func flushdb(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 广播指令
	replies := cluster.broadcast(c, cmdArgs)
	var errReply reply.ErrorReply
	// 遍历所有节点的返回结果
	for _, r := range replies {
		// 只要有一个节点返回错误，就返回错误
		if reply.IsErrReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
	}
	// 如果没有错误，返回 OK
	if errReply != nil {
		return reply.MakeOkReply()
	}
	// 如果有错误，返回错误
	return reply.MakeErrReply("error: " + errReply.Error())
}
