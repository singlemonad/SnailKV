module github.com/singlemonad/SnailKV/shardmaster/test

go 1.14

replace (
	github.com/singlemonad/SnailKV/common v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/common
	github.com/singlemonad/SnailKV/proto v0.0.0-20200504151528-9380fdb2c443 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/proto
	github.com/singlemonad/SnailKV/raft v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/raft
	github.com/singlemonad/SnailKV/labrpc v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/labrpc
)

require (
	github.com/magiconair/properties v1.8.1
	github.com/singlemonad/SnailKV v0.0.0-20200504151528-9380fdb2c443 // indirect
	github.com/singlemonad/SnailKV/proto v0.0.0-20200504151528-9380fdb2c443
	github.com/singlemonad/SnailKV/raft v0.0.0-20200504040924-99880bf76179
	github.com/singlemonad/SnailKV/labrpc v0.0.0-20200504040924-99880bf76179
	google.golang.org/grpc v1.29.1
)
