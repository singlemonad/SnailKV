module github.com/singlemonad/SnailKV/labrpc

go 1.14

replace (
  github.com/singlemonad/SnailKV/raft v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/raft
  github.com/singlemonad/SnailKV/common v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/common
  github.com/singlemonad/SnailKV/proto v0.0.0-20200504040924-99880bf76179 => /Users/yang.qu/go/src/github.com/singlemonad/SnailKV/proto
)

require (
	github.com/magiconair/properties v1.8.1
	github.com/singlemonad/SnailKV/proto v0.0.0-20200504040924-99880bf76179
	github.com/singlemonad/SnailKV/raft v0.0.0-20200504040924-99880bf76179
)
