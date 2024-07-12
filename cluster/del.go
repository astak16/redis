package cluster

import (
	"redis/interface/resp"
	"redis/resp/reply"
)

// del k1 k2 k3 k4 ...
func del(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 广播指令
	replies := cluster.broadcast(c, cmdArgs)
	var errReply reply.ErrorReply
	// 删除的数量, 默认为 0
	var deleted int64
	// 遍历所有节点的返回结果
	for _, r := range replies {
		// 如果有一个节点返回错误，就返回错误
		if reply.IsErrReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
		// 如果返回的是 IntReply，就累加
		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}
	// 如果没有错误，返回删除的数量
	if errReply != nil {
		return reply.MakeIntReply(deleted)
	}
	// 如果有错误，返回错误
	return reply.MakeErrReply("error: " + errReply.Error())
}
