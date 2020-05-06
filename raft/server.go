package raft

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/singlemonad/SnailKV/common"
	pb "github.com/singlemonad/SnailKV/proto"
)

type ServerCfg struct {
	common.ApplicationCfg
	Me                  uint64
	PeerAddress         []string
	ElectionTimeoutBase int
}

type Server struct {
	common.Application
	Cfg ServerCfg
	lg  *zap.Logger
	rf  *Raft
}

func NewServer(cfgPath string, peers []pb.RaftClient, applyCh chan pb.ApplyMsg) *Server {
	srv := &Server{}
	srv.Init()
	srv.InitConfig(cfgPath, &srv.Cfg)
	srv.InitGRpcServer()
	pb.RegisterRaftServer(srv.GRpc, srv)

	lg, _ := zap.NewProduction()
	srv.lg = lg

	srv.rf = newRaft(RaftCfg{
		Me:                  srv.Cfg.Me,
		Peers:               peers,
		NodeTotal:           uint64(len(srv.Cfg.PeerAddress)),
		Persister:           MakePersister(),
		ElectionTimeoutBase: srv.Cfg.ElectionTimeoutBase,
		ApplyCh:             applyCh,
	})
	go srv.Run()

	return srv
}

func (s *Server) Run() {
	go s.RunGRpcServer(s.Cfg.PeerAddress[s.Cfg.Me-1])

	// TODO should exit while upper exit
	sig := make(chan os.Signal)
	signal.Notify(sig, syscall.SIGINT)
	<-sig
}

func (s *Server) GetState() (uint64, bool) {
	return s.rf.GetState()
}

func (s *Server) GetRaftLogSize() uint64 {
	return s.rf.GetRaftLogSize()
}

func (s *Server) CompressLog(lastInclude uint64, snapshot []byte) {
	s.rf.CompressLog(lastInclude, snapshot)
}

func (s *Server) Start(command []byte) (uint64, uint64, bool) {
	reply, err := s.Propose(context.Background(), &pb.ProposeRequest{
		Command: command,
	})
	if err != nil {
		return 0, 0, false
	}
	return reply.Index, reply.Term, reply.Leader
}

func (s *Server) Propose(ctx context.Context, req *pb.ProposeRequest) (*pb.ProposeReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.ProposeReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, propose, req, reply)
	s.rf.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	s.lg.Sugar().Infow("RequestVoteRequest", zap.Uint64("me", s.rf.me),
		zap.Any("req", req))

	var (
		iReq  *opRequest
		reply = &pb.RequestVoteReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, requestVote, req, reply)
	s.rf.DoWork(iReq)

	err = s.handleError(ctx, iReq)

	s.lg.Sugar().Infow("RequestVoteReply", zap.Uint64("me", s.rf.me),
		zap.Any("reply", reply),
		zap.Error(err))

	return reply, err
}

func (s *Server) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	s.lg.Sugar().Infow("AppendEntriesRequest", zap.Uint64("me", s.rf.me),
		zap.Any("req", req))

	var (
		iReq  *opRequest
		reply = &pb.AppendEntriesReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, appendEntries, req, reply)
	s.rf.DoWork(iReq)

	err = s.handleError(ctx, iReq)

	s.lg.Sugar().Infow("AppendEntriesReply", zap.Uint64("me", s.rf.me),
		zap.Any("reply", reply),
		zap.Error(err))

	return reply, err
}

func (s *Server) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	var (
		iReq  *opRequest
		reply = &pb.InstallSnapshotReply{}
		err   error
	)

	ctx, cancelFunc := context.WithTimeout(ctx, gRPCTimeout)
	defer cancelFunc()

	iReq = makeOpRequest(ctx, installSnapshot, req, reply)
	s.rf.DoWork(iReq)

	err = s.handleError(ctx, iReq)
	return reply, err
}

func (s *Server) Kill() {
	s.rf.Kill()
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
		return status.Error(codes.Internal, "timeout")
	}
}

func (s *Server) createPeerClient(addr string) pb.RaftClient {
	cc, err := s.Dial(addr)
	if err != nil {
		panic(err)
	}
	return pb.NewRaftClient(cc)
}
