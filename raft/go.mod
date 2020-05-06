module github.com/singlemonad/SnailKV/raft

go 1.14

replace (
	github.com/singlemonad/SnailKV/common v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/common
	github.com/singlemonad/SnailKV/proto v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/proto
)

require (
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0 // indirect
	github.com/singlemonad/SnailKV/common v0.0.0-20200504040924-99880bf76179
	github.com/singlemonad/SnailKV/proto v0.0.0-20200504040924-99880bf76179
	github.com/singlemonad/mit6.824 v0.0.0-20200502065100-f5508b6bfe11
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.29.1
)
