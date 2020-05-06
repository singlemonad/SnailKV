package app

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/singlemonad/SnailKV/common"

	pb "github.com/singlemonad/SnailKV/proto"
	"github.com/singlemonad/SnailKV/raft"
	"github.com/singlemonad/SnailKV/shardmaster"
	"go.uber.org/zap"
)

const (
	applyChannelSize     = 10000
	opRequestChannelSize = 10000
)

type ShardKVCfg struct {
	Me                 int
	MaxRaftState       int
	Gid                int
	RaftConfigPath     string
	RaftPeers          []pb.RaftClient
	ShardMasterClients []pb.ShardMasterClient
}

type ShardKV struct {
	lg *zap.SugaredLogger
	mu sync.Mutex
	me int

	Cfg ShardKVCfg

	rf *raft.Server

	kvs map[string]string

	applyCh chan pb.ApplyMsg
	loopCh  chan *opRequest
	exitCh  chan interface{}

	gid int

	maxRaftState int // snapshot if log grows this big

	masterClerk *shardmaster.Clerk

	latestConfig pb.ClusterConfig
	zones        map[int]bool

	inReq map[string]*opRequest
}

func newShardKV(cfg ShardKVCfg) *ShardKV {
	kv := &ShardKV{}

	lg, _ := zap.NewProduction()
	kv.lg = lg.Sugar()

	kv.me = cfg.Me
	kv.maxRaftState = cfg.MaxRaftState
	kv.gid = cfg.Gid

	kv.applyCh = make(chan pb.ApplyMsg, applyChannelSize)
	kv.rf = raft.NewServer(cfg.RaftConfigPath, cfg.RaftPeers, kv.applyCh)

	kv.kvs = make(map[string]string)

	kv.loopCh = make(chan *opRequest, opRequestChannelSize)
	kv.exitCh = make(chan interface{})

	kv.zones = make(map[int]bool)

	kv.inReq = make(map[string]*opRequest)

	kv.masterClerk = shardmaster.MakeClerk(cfg.ShardMasterClients)

	go kv.mainLoop()

	return kv
}

func (kv *ShardKV) DoWork(req *opRequest) {
	kv.loopCh <- req
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.exitCh)
}

func (kv *ShardKV) mainLoop() {
	defer func() {
		kv.lg.Infow("main loop exit", zap.Int("me", kv.me))
	}()

	shardTicker := time.NewTicker(time.Millisecond * 100)
	snapshotTicker := time.NewTicker(time.Millisecond * 50)
	for {
		select {
		case <-shardTicker.C:
			kv.doShardChange()
		case <-snapshotTicker.C:
			kv.doSnapshot()
		case req := <-kv.loopCh:
			if kv.ownershipVerify(req) == false {
				kv.rejectNotOwnReq(req)
				continue
			}

			switch req.typ {
			case GetOp:
				kv.get(req)
			case PutOp:
				kv.put(req)
			case AppendOp:
				kv.append(req)
			case DeleteOp:
				kv.delete(req)
			case MigrateOp:
				kv.migrate(req)
			}
		case entry := <-kv.applyCh:
			kv.apply(entry)
		case <-kv.exitCh:
			return
		}
	}
}

func (kv *ShardKV) ownershipVerify(req *opRequest) bool {
	var key string
	switch req.typ {
	case GetOp:
		key = req.args.(*pb.GetRequest).Key
	case PutOp:
		key = req.args.(*pb.PutRequest).Key
	case AppendOp:
		key = req.args.(*pb.AppendRequest).Key
	case DeleteOp:
		key = req.args.(*pb.DeleteRequest).Key
	case MigrateOp:
		// TODO finish
	}

	var zone int
	zone = kv.key2shard(key)

	if _, ok := kv.zones[zone]; ok {
		return true
	}
	return false
}

func (kv *ShardKV) rejectNotOwnReq(req *opRequest) {
	req.errorCh <- common.ErrWrongLeader
}

func (kv *ShardKV) doSnapshot() {
	if kv.maxRaftState == -1 {
		return
	}

	if kv.rf.GetRaftLogSize() > uint64(kv.maxRaftState) {
		// TODO finish it
		kv.rf.CompressLog(0, kv.materialize())
	}
}

func (kv *ShardKV) doShardChange() {
	// TODO finish it
}

func (kv *ShardKV) doAddShards() {
	// TODO finish it
}

func (kv *ShardKV) doDelShards() {
	// TODO finish it
}

func (kv *ShardKV) get(req *opRequest) {
	args := req.args.(*pb.GetRequest)
	reply := req.reply.(*pb.GetReply)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	if val, ok := kv.kvs[args.Key]; ok {
		reply.Value = val
		req.errorCh <- nil
		return
	}
	req.errorCh <- common.ErrNotExist
}

func (kv *ShardKV) put(req *opRequest) {
	args := req.args.(*pb.PutRequest)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	uuid := extractUUID(req.ctx)
	kv.traceReq(uuid, req)
	kv.rf.Propose(context.Background(), &pb.ProposeRequest{
		Command: materRaftOp(&raftOp{
			Uuid: uuid,
			Typ:  PutOp,
			Args: args,
		}),
	})
}

func (kv *ShardKV) append(req *opRequest) {
	args := req.args.(*pb.AppendRequest)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	uuid := extractUUID(req.ctx)
	kv.traceReq(uuid, req)
	kv.rf.Propose(context.Background(), &pb.ProposeRequest{
		Command: materRaftOp(&raftOp{
			Uuid: extractUUID(req.ctx),
			Typ:  AppendOp,
			Args: args,
		}),
	})
}

func (kv *ShardKV) delete(req *opRequest) {
	args := req.args.(*pb.DeleteRequest)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	uuid := extractUUID(req.ctx)
	kv.traceReq(uuid, req)
	kv.rf.Propose(context.Background(), &pb.ProposeRequest{
		Command: materRaftOp(&raftOp{
			Uuid: extractUUID(req.ctx),
			Typ:  DeleteOp,
			Args: args,
		}),
	})
}

func (kv *ShardKV) migrate(req *opRequest) {
	args := req.args.(*pb.MigrateRequest)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		req.errorCh <- common.ErrWrongLeader
		return
	}

	uuid := extractUUID(req.ctx)
	kv.traceReq(uuid, req)
	kv.rf.Propose(context.Background(), &pb.ProposeRequest{
		Command: materRaftOp(&raftOp{
			Uuid: extractUUID(req.ctx),
			Typ:  MigrateOp,
			Args: args,
		}),
	})
}

func (kv *ShardKV) apply(msg pb.ApplyMsg) {
	if msg.UseSnapshot {
		kv.kvs = kv.deMaterialize(msg.Snapshot)
		return
	}

	command := deMaterRaftOp(msg.Content)
	switch command.Typ {
	case PutOp:
		kv.applyPut(command.Uuid, command.Args.(*pb.PutRequest))
	case AppendOp:
		kv.applyAppend(command.Uuid, command.Args.(*pb.AppendRequest))
	case DeleteOp:
		kv.applyDelete(command.Uuid, command.Args.(*pb.DeleteRequest))
	case MigrateOp:
		kv.applyMigrate(command.Uuid, command.Args.(*pb.MigrateRequest))
	}
}

func (kv *ShardKV) applyPut(uuid string, args *pb.PutRequest) {
	kv.kvs[args.Key] = args.Value
	kv.weakUpReq(uuid)
}

func (kv *ShardKV) applyAppend(uuid string, args *pb.AppendRequest) {
	if old, ok := kv.kvs[args.Key]; ok {
		kv.kvs[args.Key] = old + args.Value
	} else {
		kv.kvs[args.Key] = args.Value
	}
	kv.weakUpReq(uuid)
}

func (kv *ShardKV) applyDelete(uuid string, args *pb.DeleteRequest) {
	if _, ok := kv.kvs[args.Key]; ok {
		delete(kv.kvs, args.Key)
	}
	kv.weakUpReq(uuid)
}

func (kv *ShardKV) applyMigrate(uuid string, args *pb.MigrateRequest) {
	// TODO finish it
}

func (kv *ShardKV) isLeader() bool {
	if _, isLeader := kv.rf.GetState(); isLeader {
		return true
	}
	return false
}

func (kv *ShardKV) traceReq(uuid string, req *opRequest) {
	kv.inReq[uuid] = req
}

func (kv *ShardKV) weakUpReq(uuid string) {
	if req, ok := kv.inReq[uuid]; ok {
		req.errorCh <- nil
	}
}

func (kv *ShardKV) materialize() []byte {
	writer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writer)

	if err := encoder.Encode(len(kv.kvs)); err != nil {
		panic(err)
	}
	for k, v := range kv.kvs {
		if err := encoder.Encode(k); err != nil {
			panic(err)
		}
		if err := encoder.Encode(v); err != nil {
			panic(err)
		}
	}
	return writer.Bytes()
}

func (kv *ShardKV) deMaterialize([]byte) map[string]string {
	reader := new(bytes.Buffer)
	decoder := gob.NewDecoder(reader)

	var length int
	if err := decoder.Decode(&length); err != nil {
		panic(err)
	}
	kvs := make(map[string]string)
	for i := 0; i < length; i++ {
		var k, v string
		if err := decoder.Decode(&k); err != nil {
			panic(err)
		}
		if err := decoder.Decode(&v); err != nil {
			panic(err)
		}
		kvs[k] = v
	}
	return kvs
}

func (kv *ShardKV) key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= len(kv.latestConfig.Zones)
	return shard
}
