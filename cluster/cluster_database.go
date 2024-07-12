package cluster

import (
	"context"
	"redis/config"
	"redis/consistenthash"
	database2 "redis/database"
	databaseface "redis/interface/database"
	"redis/interface/resp"
	"redis/lib/logger"
	"redis/resp/reply"
	"strings"

	pool "github.com/jolestar/go-commons-pool/v2"
)

type ClusterDatabase struct {
	self           string                      // 记录自己的名称和地址
	nodes          []string                    // 集群节点的切片
	peerPicker     *consistenthash.NodeMap     // 一致性 hash
	peerConnection map[string]*pool.ObjectPool // 池化工具 -> map 的 key 是节点地址
	db             databaseface.Database       // 集群层 -> StandaloneDatabase
}

func MakeClusterDatabase() *ClusterDatabase {
	// 初始化 clusterDatabase
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,            // 记录自己的名称和地址
		db:             database2.NewStandaloneDatabase(), // 初始化自生的 StandaloneDatabase
		peerPicker:     consistenthash.NewNodeMap(nil),    // 初始化一致性 hash
		peerConnection: make(map[string]*pool.ObjectPool), // 初始化连接池
	}
	// 添加节点，长度：peers + self
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	// 添加节点到一致性 hash 中
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	// 初始化连接池
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	// 将 nodes 保存在 cluster 中
	cluster.nodes = nodes
	return cluster
}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter()

func (cluster *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	// 恢复 panic
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = reply.MakeUnknownErrReply()
		}
	}()
	// 拿到用户输入的指令
	cmdName := strings.ToLower(string(args[0]))
	// 从 router 中获取指令
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("not supported cmd")
	}
	// 执行指令
	result = cmdFunc(cluster, client, args)
	return
}

func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
