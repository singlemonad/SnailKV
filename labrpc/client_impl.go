package labrpc

import (
	"context"
	"errors"
	"time"

	pb "github.com/singlemonad/SnailKV/proto"
	"google.golang.org/grpc"
)

const callTimeout = time.Millisecond * 50

func (e *ClientEnd) callWithTimeout(svcMeth string, ctx context.Context, in interface{}) (interface{}, error) {
	var reply interface{}
	var err error

	finishCh := make(chan interface{})
	go func(finishCh chan interface{}) {
		reply, err = e.Call(svcMeth, ctx, in)
		finishCh <- struct{}{}
	}(finishCh)

	select {
	case <-finishCh:
		return reply, err
	case <-time.After(callTimeout):
		return nil, errors.New("timeout")
	}
}

// implement interface pb.RaftClient
func (e *ClientEnd) RequestVote(ctx context.Context, in *pb.RequestVoteRequest, opts ...grpc.CallOption) (*pb.RequestVoteReply, error) {
	res, err := e.callWithTimeout("RaftServer.RequestVote", ctx, in)
	if err == nil {
		return res.(*pb.RequestVoteReply), nil
	}
	return nil, err
}

func (e *ClientEnd) AppendEntries(ctx context.Context, in *pb.AppendEntriesRequest, opts ...grpc.CallOption) (*pb.AppendEntriesReply, error) {
	res, err := e.callWithTimeout("RaftServer.AppendEntries", ctx, in)
	if err == nil {
		return res.(*pb.AppendEntriesReply), nil
	}
	return nil, err
}

func (e *ClientEnd) InstallSnapshot(ctx context.Context, in *pb.InstallSnapshotRequest, opts ...grpc.CallOption) (*pb.InstallSnapshotReply, error) {
	res, err := e.callWithTimeout("RaftServer.InstallSnapshot", ctx, in)
	if err == nil {
		return res.(*pb.InstallSnapshotReply), nil
	}
	return nil, err
}

// implement interface pb.ShardMasterClient
func (e *ClientEnd) Join(ctx context.Context, in *pb.JoinRequest, opts ...grpc.CallOption) (*pb.JoinReply, error) {
	res, err := e.callWithTimeout("ShardMaster.Join", ctx, in)
	if err == nil {
		return res.(*pb.JoinReply), nil
	}
	return nil, err
}

func (e *ClientEnd) Leave(ctx context.Context, in *pb.LeaveRequest, opts ...grpc.CallOption) (*pb.LeaveReply, error) {
	res, err := e.callWithTimeout("ShardMaster.Leave", ctx, in)
	if err == nil {
		return res.(*pb.LeaveReply), nil
	}
	return nil, err
}

func (e *ClientEnd) Move(ctx context.Context, in *pb.MoveRequest, opts ...grpc.CallOption) (*pb.MoveReply, error) {
	res, err := e.callWithTimeout("ShardMaster.Move", ctx, in)
	if err == nil {
		return res.(*pb.MoveReply), nil
	}
	return nil, err
}

func (e *ClientEnd) Query(ctx context.Context, in *pb.QueryRequest, opts ...grpc.CallOption) (*pb.QueryReply, error) {
	res, err := e.callWithTimeout("ShardMaster.Query", ctx, in)
	if err == nil {
		return res.(*pb.QueryReply), nil
	}
	return nil, err
}

// implement interface pb.ShardKVClient
func (e *ClientEnd) Get(ctx context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetReply, error) {
	res, err := e.callWithTimeout("ShardKV.Get", ctx, in)
	if err == nil {
		return res.(*pb.GetReply), nil
	}
	return nil, err
}

func (e *ClientEnd) Put(ctx context.Context, in *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutReply, error) {
	res, err := e.callWithTimeout("ShardKV.Query", ctx, in)
	if err == nil {
		return res.(*pb.PutReply), nil
	}
	return nil, err
}

func (e *ClientEnd) Append(ctx context.Context, in *pb.AppendRequest, opts ...grpc.CallOption) (*pb.AppendReply, error) {
	res, err := e.callWithTimeout("ShardKV.Append", ctx, in)
	if err == nil {
		return res.(*pb.AppendReply), nil
	}
	return nil, err
}

func (e *ClientEnd) Delete(ctx context.Context, in *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteReply, error) {
	res, err := e.callWithTimeout("ShardKV.Delete", ctx, in)
	if err == nil {
		return res.(*pb.DeleteReply), nil
	}
	return nil, err
}

func (e *ClientEnd) Migrate(ctx context.Context, in *pb.MigrateRequest, opts ...grpc.CallOption) (*pb.MigrateReply, error) {
	res, err := e.callWithTimeout("ShardKV.Migrate", ctx, in)
	if err == nil {
		return res.(*pb.MigrateReply), nil
	}
	return nil, err
}
