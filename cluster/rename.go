package cluster

import (
	"redis/interface/resp"
	"redis/resp/reply"
)

// rename k1 k2
func rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 参数校验
	if len(cmdArgs) != 3 {
		return reply.MakeErrReply("ERR wrong number args")
	}
	// 获取修改之前的名字
	src := string(cmdArgs[1])
	// 获取修改之后的名字
	dest := string(cmdArgs[2])
	// 根据 src 获取节点
	srcPeer := cluster.peerPicker.PickNode(src)
	// 根据 dest 获取节点
	destPeer := cluster.peerPicker.PickNode(dest)
	// 如果 srcPeer 和 destPeer 不一样，说明不在同一个节点
	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within on peer")
	}
	// 在同一个节点，则转发指令
	return cluster.relay(srcPeer, c, cmdArgs)
}
