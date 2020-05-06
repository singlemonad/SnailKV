package app

import (
	"bytes"
	"context"
	"encoding/gob"
)

type OperationType = int

const (
	GetOp     = 1
	PutOp     = 2
	AppendOp  = 3
	DeleteOp  = 4
	MigrateOp = 5
)

type opRequest struct {
	ctx     context.Context
	typ     OperationType
	args    interface{}
	reply   interface{}
	errorCh chan interface{}
}

func makeOpRequest(ctx context.Context, typ OperationType, args interface{}, reply interface{}) *opRequest {
	return &opRequest{
		ctx:     ctx,
		typ:     typ,
		args:    args,
		reply:   reply,
		errorCh: make(chan interface{}),
	}
}

type raftOp struct {
	Uuid string
	Typ  OperationType
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
