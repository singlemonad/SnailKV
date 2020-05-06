package raft

import (
	"context"
	"time"

	pb "github.com/singlemonad/SnailKV/proto"
)

const (
	gRPCTimeout = time.Second * 2
)

type Clerk struct {
	servers []pb.RaftClient
}

func MakeClerk(servers []pb.RaftClient) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	return ck
}

func (c *Clerk) RequestVote(ctx context.Context, peerID uint64, req *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	return c.servers[peerID].RequestVote(ctx, req)
}

func (c *Clerk) AppendEntries(ctx context.Context, peerID uint64, req *pb.AppendEntriesRequest) (*pb.AppendEntriesReply, error) {
	return c.servers[peerID].AppendEntries(ctx, req)
}

func (c *Clerk) InstallSnapshot(ctx context.Context, peerID uint64, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotReply, error) {
	return c.servers[peerID].InstallSnapshot(ctx, req)
}
