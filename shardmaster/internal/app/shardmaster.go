package app

import (
	"bytes"
	"context"
	"encoding/gob"
	"uframework/message/protobuf/proto"

	"github.com/singlemonad/SnailKV/common"

	pb "github.com/singlemonad/SnailKV/proto"
	"github.com/singlemonad/snailkv/raft"
	"go.uber.org/zap"
)

const (
	hashReplicas = 400

	loopChannelSize  = 10000
	applyChannelSize = 10000
)

type shardMasterCfg struct {
	Me             int
	MaxRaftState   int
	RaftConfigPath string
	RaftPeers      []pb.RaftClient
}

type shardMaster struct {
	me int

	lg *zap.SugaredLogger

	Cfg shardMasterCfg

	rf *raft.Server

	clusterCfg    map[uint64]*pb.ClusterConfig
	latestVersion uint64

	hash *Hash

	inReq map[string]*opRequest

	loopCh  chan *opRequest
	applyCh chan pb.ApplyMsg

	exitCh chan interface{}
}

func newShardMaster(cfg shardMasterCfg) *shardMaster {
	sm := &shardMaster{}
	sm.Cfg = cfg
	sm.me = sm.Cfg.Me
	lg, _ := zap.NewProduction()
	sm.lg = lg.Sugar()
	sm.clusterCfg = make(map[uint64]*pb.ClusterConfig)
	sm.hash = NewHash(hashReplicas, HashUInt64)
	sm.inReq = make(map[string]*opRequest)
	sm.loopCh = make(chan *opRequest, loopChannelSize)
	sm.applyCh = make(chan pb.ApplyMsg, applyChannelSize)
	sm.exitCh = make(chan interface{})

	sm.rf = raft.NewServer(sm.Cfg.RaftConfigPath, sm.Cfg.RaftPeers, sm.applyCh)

	return sm
}

func (sm *shardMaster) DoWork(req *opRequest) {
	sm.loopCh <- req
}

func (sm *shardMaster) Kill() {
	sm.rf.Kill()
	close(sm.exitCh)
}

func (sm *shardMaster) mainLoop() {
	defer func() {
		sm.lg.Info("shard master exit main loop", zap.Int("me", sm.me))
	}()

	for {
		select {
		case req := <-sm.loopCh:
			switch req.typ {
			case join:
				sm.join(req)
			case leave:
				sm.leave(req)
			case move:
				sm.move(req)
			case query:
				sm.query(req)
			}
		case entry := <-sm.applyCh:
			sm.apply(entry)
		case <-sm.exitCh:
			return
		}
	}
}

func (sm *shardMaster) join(req *opRequest) {
	args := req.args.(*pb.JoinRequest)
	if !sm.isLeader() {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	sm.rf.Propose(context.Background(), &pb.ProposeRequest{
		Command: materRaftOp(&raftOp{
			Typ:  join,
			Args: args,
		}),
	})

}

func (sm *shardMaster) leave(req *opRequest) {
	args := req.args.(*pb.LeaveRequest)
	if !sm.isLeader() {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	sm.rf.Propose(context.Background(), &pb.ProposeRequest{
		Command: materRaftOp(&raftOp{
			Typ:  leave,
			Args: args,
		}),
	})
}

func (sm *shardMaster) move(req *opRequest) {
	args := req.args.(*pb.MoveRequest)
	if !sm.isLeader() {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	sm.rf.Propose(context.Background(), &pb.ProposeRequest{
		Command: materRaftOp(&raftOp{
			Typ:  move,
			Args: args,
		}),
	})
}

func (sm *shardMaster) query(req *opRequest) {
	args := req.args.(*pb.QueryRequest)
	reply := req.reply.(*pb.QueryReply)
	if !sm.isLeader() {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	if args.Version < 0 {
		reply.Config = sm.cloneClusterCfg(sm.latestVersion)
		req.errorCh <- nil
		return
	} else if _, ok := sm.clusterCfg[args.Version]; !ok {
		req.errorCh <- common.ErrNotExist
		return
	} else {
		reply.Config = sm.clusterCfg[args.Version]
		req.errorCh <- nil
		return
	}
}

func (sm *shardMaster) apply(entry pb.ApplyMsg) {
	if entry.UseSnapshot {
		sm.deMaterlize(entry.Snapshot)
		return
	}

	command := deMaterRaftOp(entry.Content)
	switch command.Typ {
	case join:
		sm.applyJoin(command.Args.(pb.JoinRequest))
	case leave:
		sm.applyLeave(command.Args.(pb.LeaveRequest))
	case move:
		sm.applyMove(command.Args.(pb.MoveRequest))
	}
}

func (sm *shardMaster) cloneClusterCfg(version uint64) *pb.ClusterConfig {
	if _, ok := sm.clusterCfg[version]; !ok {
		return &pb.ClusterConfig{
			Groups:  make(map[uint64]*pb.StringList),
			Zones:   make(map[uint64]uint64),
			Version: version,
		}
	}
	return proto.Clone(sm.clusterCfg[version]).(*pb.ClusterConfig)
}

func (sm *shardMaster) applyJoin(args pb.JoinRequest) {
	var (
		cfg *pb.ClusterConfig
	)

	cfg = sm.cloneClusterCfg(sm.latestVersion)
	cfg.Version++
	newGIDs := make([]uint64, 0)
	for gid, servers := range args.Groups {
		cfg.Groups[gid] = servers
		newGIDs = append(newGIDs, gid)
	}
	sm.hash.Add(newGIDs)
	sm.reHash(cfg)
	sm.clusterCfg[cfg.Version] = cfg
	sm.latestVersion = cfg.Version
}

func (sm *shardMaster) applyLeave(args pb.LeaveRequest) {
	var (
		cfg *pb.ClusterConfig
	)

	cfg = sm.cloneClusterCfg(sm.latestVersion)
	cfg.Version++
	for _, gid := range args.GIDs {
		delete(cfg.Groups, gid)
	}
	sm.hash.Delete(args.GIDs)
	sm.reHash(cfg)
	sm.clusterCfg[cfg.Version] = cfg
	sm.latestVersion = cfg.Version
}

func (sm *shardMaster) applyMove(args pb.MoveRequest) {
	var (
		cfg *pb.ClusterConfig
	)

	cfg = sm.cloneClusterCfg(sm.latestVersion)
	cfg.Version++
	if _, ok := cfg.Groups[args.GID]; ok {
		cfg.Zones[args.ZoneID] = args.GID
	}
	sm.reHash(cfg)
	sm.clusterCfg[cfg.Version] = cfg
	sm.latestVersion = cfg.Version
}

func (sm *shardMaster) reHash(cfg *pb.ClusterConfig) {
	for zone, _ := range cfg.Zones {
		cfg.Zones[zone] = sm.hash.Hash(zone)
	}
}

func (sm *shardMaster) isLeader() bool {
	_, isLeader := sm.rf.GetState()
	return isLeader
}

func (sm *shardMaster) materlize() []byte {
	writer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writer)

	if err := encoder.Encode(len(sm.clusterCfg)); err != nil {
		panic(err)
	}
	for _, cfg := range sm.clusterCfg {
		if err := encoder.Encode(cfg); err != nil {
			panic(err)
		}
	}
	return writer.Bytes()
}

func (sm *shardMaster) deMaterlize([]byte) {
	reader := new(bytes.Buffer)
	decoder := gob.NewDecoder(reader)

	var length int
	if err := decoder.Decode(&length); err != nil {
		panic(err)
	}
	sm.clusterCfg = make(map[uint64]*pb.ClusterConfig)
	for i := 0; i < length; i++ {
		var cfg pb.ClusterConfig
		if err := decoder.Decode(&cfg); err != nil {
			panic(err)
		}
		sm.clusterCfg[cfg.Version] = &cfg
	}
	return
}
