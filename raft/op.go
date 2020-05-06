package raft

import "context"

type operationType = int

const (
	requestVote     operationType = 1
	appendEntries   operationType = 2
	installSnapshot operationType = 3
	propose         operationType = 4
)

type opRequest struct {
	ctx     context.Context
	typ     operationType
	args    interface{}
	reply   interface{}
	errorCh chan error
}

func makeOpRequest(ctx context.Context, typ operationType, args, reply interface{}) *opRequest {
	return &opRequest{
		ctx:     ctx,
		typ:     typ,
		args:    args,
		reply:   reply,
		errorCh: make(chan error),
	}
}
