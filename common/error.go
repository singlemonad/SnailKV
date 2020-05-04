package common

import (
	"errors"

	"google.golang.org/grpc/codes"
)

var (
	WrongLeader codes.Code = 1001
	NotExist    codes.Code = 1002
)

var (
	ErrWrongLeader = errors.New("wrong leader")
	ErrNotExist    = errors.New("not exist")
)
