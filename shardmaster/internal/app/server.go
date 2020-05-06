package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/singlemonad/SnailKV/raft"

	"github.com/singlemonad/SnailKV/common"
	pb "github.com/singlemonad/SnailKV/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	grpcTimeout = time.Second * 3
)

type ServerCfg struct {
	common.ApplicationCfg
	Me             int
	MaxRaftState   int
	RaftConfigPath string
	RaftPeers      []string
}

type Server struct {
	common.Application
	lg  *zap.SugaredLogger
	Cfg ServerCfg
	sm  *shardMaster
}

// For test
func NewFakeServer(configPath string, raftPeers []pb.RaftClient) *Server {
	srv := &Server{}
	lg, _ := zap.NewProduction()
	srv.lg = lg.Sugar()

	srv.Init()
	srv.InitConfig(configPath, &srv.Cfg)
	srv.sm = newShardMaster(shardMasterCfg{
		Me:             srv.Cfg.Me,
		MaxRaftState:   srv.Cfg.MaxRaftState,
		RaftConfigPath: srv.Cfg.RaftConfigPath,
		RaftPeers:      raftPeers,
	})
	srv.InitGRpcServer()
	pb.RegisterShardMasterServer(srv.GRpc, srv)
	return srv
}

func NewServer() *Server {
	srv := &Server{}
	lg, _ := zap.NewProduction()
	srv.lg = lg.Sugar()

	srv.Init()
	srv.InitGRpcServer()
	pb.RegisterShardMasterServer(srv.GRpc, srv)

	// create raft client array
	raftClients := make([]pb.RaftClient, 0)
	for _, raftAddr := range srv.Cfg.RaftPeers {
		cc, err := srv.Dial(raftAddr)
		if err != nil {
			panic(err)
		}
		raftClients = append(raftClients, pb.NewRaftClient(cc))
	}
	srv.sm = newShardMaster(shardMasterCfg{
		Me:             srv.Cfg.Me,
		MaxRaftState:   srv.Cfg.MaxRaftState,
		RaftConfigPath: srv.Cfg.RaftConfigPath,
		RaftPeers:      raftClients,
	})

	go srv.Run()
	return srv
}

// for test
func (s *Server) GetRaftServer() *raft.Server {
	return s.sm.rf
}

func (s *Server) Run() {
	go s.RunGRpcServer("")

	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT)
	<-sig
}

func (s *Server) Kill() {
	s.sm.Kill()
}

func (s *Server) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.JoinReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, grpcTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, join, req, reply)
	s.sm.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.LeaveReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, grpcTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, leave, req, reply)
	s.sm.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Move(ctx context.Context, req *pb.MoveRequest) (*pb.MoveReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.MoveReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, grpcTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, move, req, reply)
	s.sm.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.QueryReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, grpcTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, query, req, reply)
	s.sm.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) GetState() (uint64, bool) {
	return s.sm.rf.GetState()
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
