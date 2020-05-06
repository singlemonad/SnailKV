package app

import (
	"bytes"
	"context"
	"encoding/gob"
)

type operationType = int

const (
	join  operationType = 1
	leave operationType = 2
	move  operationType = 3
	query operationType = 4
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

type raftOp struct {
	Typ  operationType
	Args interface{}
}

func materRaftOp(op *raftOp) []byte {
	writer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writer)
	if err := encoder.Encode(op); err != nil {
		panic(err)
	}
	return writer.Bytes()
}

func deMaterRaftOp(data []byte) *raftOp {
	var op raftOp
	reader := new(bytes.Buffer)
	decoder := gob.NewDecoder(reader)
	if err := decoder.Decode(&op); err != nil {
		panic(err)
	}
	return &op
}
