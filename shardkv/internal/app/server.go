package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/singlemonad/SnailKV/common"
	pb "github.com/singlemonad/SnailKV/proto"
	"github.com/singlemonad/SnailKV/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const gRPCTimeout = time.Millisecond * 50

type ServerCfg struct {
	Me                 int
	MaxRaftState       int
	Gid                int
	RaftConfigPath     string
	RaftPeers          []string
	ShardMasterClients []string
}

type Server struct {
	lg  *zap.SugaredLogger
	Cfg ServerCfg
	common.Application
	kv *ShardKV
}

// For test
func NewFakeServer(configPath string, raftPeers []pb.RaftClient, masters []pb.ShardMasterClient) *Server {
	srv := &Server{}
	lg, _ := zap.NewProduction()
	srv.lg = lg.Sugar()

	srv.Init()
	srv.InitConfig(configPath, &srv.Cfg)
	srv.kv = newShardKV(ShardKVCfg{
		Me:                 srv.Cfg.Me,
		MaxRaftState:       srv.Cfg.MaxRaftState,
		Gid:                srv.Cfg.Gid,
		RaftConfigPath:     srv.Cfg.RaftConfigPath,
		RaftPeers:          raftPeers,
		ShardMasterClients: masters,
	})
	srv.InitGRpcServer()
	pb.RegisterShardKVServer(srv.GRpc, srv)
	return srv
}

func NewServer() *Server {
	srv := &Server{}
	lg, _ := zap.NewProduction()
	srv.lg = lg.Sugar()

	srv.Init()
	srv.InitConfig("", &srv.Cfg)

	srv.InitGRpcServer()
	pb.RegisterShardKVServer(srv.GRpc, srv)

	// create master client array
	masterClients := make([]pb.ShardMasterClient, 0)
	for _, masterAddr := range srv.Cfg.ShardMasterClients {
		cc, err := srv.Dial(masterAddr)
		if err != nil {
			panic(err)
		}
		masterClients = append(masterClients, pb.NewShardMasterClient(cc))
	}

	// create raft client array
	raftClients := make([]pb.RaftClient, 0)
	for _, raftAddr := range srv.Cfg.RaftPeers {
		cc, err := srv.Dial(raftAddr)
		if err != nil {
			panic(err)
		}
		raftClients = append(raftClients, pb.NewRaftClient(cc))
	}
	srv.kv = newShardKV(ShardKVCfg{
		Me:                 srv.Cfg.Me,
		MaxRaftState:       srv.Cfg.MaxRaftState,
		Gid:                srv.Cfg.Gid,
		RaftConfigPath:     srv.Cfg.RaftConfigPath,
		RaftPeers:          raftClients,
		ShardMasterClients: masterClients,
	})

	go srv.Run()
	return srv
}

// For test
func (s *Server) GetRaftServer() *raft.Server {
	return s.kv.rf
}

func (s *Server) Run() {
	go s.RunGRpcServer("")

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT)
	<-sig
}
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.GetReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, GetOp, req, reply)
	s.kv.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.PutReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, PutOp, req, reply)
	s.kv.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.AppendReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, AppendOp, req, reply)
	s.kv.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.DeleteReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, DeleteOp, req, reply)
	s.kv.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Migrate(ctx context.Context, req *pb.MigrateRequest) (*pb.MigrateReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.MigrateReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, MigrateOp, req, reply)
	s.kv.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) handleError(ctx context.Context, iReq *opRequest) error {
	select {
	case err := <-iReq.errorCh:
		switch err {
		case nil:
			return nil
		case common.ErrWrongLeader:
			return status.Error(common.WrongLeader, "wrong leader")
		default:
			return status.Error(codes.Internal, "internal error")
		}
	case <-ctx.Done():
		return status.Error(codes.Internal, "shard master timeout")
	}
}

func extractUUID(ctx context.Context) string {
	return ctx.Value("UUID").(string)
}
