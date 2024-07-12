package cluster

import (
	"context"
	"errors"
	"redis/interface/resp"
	"redis/lib/utils"
	"redis/resp/client"
	"redis/resp/reply"
	"strconv"
)

// 用来获取节点的连接
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	// 从 peerConnection 中获取连接
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection not found")
	}
	// 从连接池中获取借用一个连接
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	// 断言连接类型
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type")
	}
	// 返回连接
	return c, err
}

// 用来归还连接
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	// 从 peerConnection 中获取连接
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection not found")
	}
	// 归还连接
	return pool.ReturnObject(context.Background(), peerClient)
}

// 用来转发指令
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	// 如果 peer 是自己，直接执行指令
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	// 获取节点的连接
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	// 归还连接
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	// 选择节点数据库
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	// 发送指令
	return peerClient.Send(args)
}

// 用来广播指令
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	results := make(map[string]resp.Reply)
	// 遍历节点
	for _, node := range cluster.nodes {
		// 转发指令
		result := cluster.relay(node, c, args)
		results[node] = result
	}
	return results
}
