package shardmaster

//
// Shardmaster clerk.
//

import (
	"context"
	"crypto/rand"
	"math/big"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/singlemonad/SnailKV/common"
	pb "github.com/singlemonad/SnailKV/proto"
	"google.golang.org/grpc/status"
)

const (
	grpcTimoute = time.Second * 2
)

type Clerk struct {
	servers []pb.ShardMasterClient
}

func MakeClerk(servers []pb.ShardMasterClient) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	return ck
}

func (ck *Clerk) Join(servers map[uint64][]string) {
	gps := make(map[uint64]*pb.StringList)
	for gid, hosts := range servers {
		gps[gid] = &pb.StringList{Strings: hosts}
	}
	var (
		args = &pb.JoinRequest{
			Groups: gps,
		}
		err error
	)

	ctx, cancelF := context.WithTimeout(context.Background(), grpcTimoute)
	defer cancelF()

	uuid, _ := uuid.NewV4()
	ctx = context.WithValue(ctx, "uuid", uuid.String())
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if _, err = srv.Join(ctx, args); err != nil {
				var st *status.Status
				var ok bool
				if st, ok = status.FromError(err); !ok {
					//
				}

				if st.Code() == common.WrongLeader {
					continue
				} else {
					break
				}
			} else {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []uint64) {
	var (
		args = &pb.LeaveRequest{
			GIDs: gids,
		}
		err error
	)

	ctx, cancelF := context.WithTimeout(context.Background(), grpcTimoute)
	defer cancelF()

	uuid, _ := uuid.NewV4()
	ctx = context.WithValue(ctx, "uuid", uuid.String())
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if _, err = srv.Leave(ctx, args); err != nil {
				var st *status.Status
				var ok bool
				if st, ok = status.FromError(err); !ok {
					//
				}

				if st.Code() == common.WrongLeader {
					continue
				} else {
					break
				}
			} else {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(zone uint64, gid uint64) {
	var (
		args = &pb.MoveRequest{
			ZoneID: zone,
			GID:    gid,
		}
		err error
	)

	ctx, cancelF := context.WithTimeout(context.Background(), grpcTimoute)
	defer cancelF()

	uuid, _ := uuid.NewV4()
	ctx = context.WithValue(ctx, "uuid", uuid.String())
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if _, err = srv.Move(ctx, args); err != nil {
				var st *status.Status
				var ok bool
				if st, ok = status.FromError(err); !ok {
					//
				}

				if st.Code() == common.WrongLeader {
					continue
				} else {
					break
				}
			} else {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (ck *Clerk) Query(version uint64) *pb.ClusterConfig {
	var (
		args  = &pb.QueryRequest{Version: version}
		reply = &pb.QueryReply{}
		err   error
	)

	ctx, cancelF := context.WithTimeout(context.Background(), grpcTimoute)
	defer cancelF()

	uuid, _ := uuid.NewV4()
	ctx = context.WithValue(ctx, "uuid", uuid.String())
	for {
		// try each known server.
		for _, srv := range ck.servers {
			if reply, err = srv.Query(ctx, args); err != nil {
				var st *status.Status
				var ok bool
				if st, ok = status.FromError(err); !ok {
					//
				}

				if st.Code() == common.WrongLeader {
					continue
				} else {
					break
				}
			} else {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
