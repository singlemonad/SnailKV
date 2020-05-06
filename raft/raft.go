package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"
	"sort"
	"sync"
	"time"

	pb "github.com/singlemonad/SnailKV/proto"
	"github.com/singlemonad/SnailKV/raft/log"
	"go.uber.org/zap"
)

const (
	loopChannelSize = 10000
)

type stepFunc = func()

type RaftCfg struct {
	Me                  uint64
	Peers               []pb.RaftClient
	NodeTotal           uint64
	Persister           *Persister
	ElectionTimeoutBase int
	ApplyCh             chan pb.ApplyMsg
}

type Raft struct {
	lg        *zap.SugaredLogger
	mu        sync.RWMutex
	nodeTotal uint64
	client    *Clerk
	Cfg       RaftCfg
	persister *Persister

	role pb.Role
	me   uint64

	term    uint64
	voteFor uint64

	leader uint64

	commit  uint64
	applied uint64

	stepFunc stepFunc

	raftLog log.RaftLog
	applyCh chan pb.ApplyMsg
	loopCh  chan *opRequest
	exitCh  chan interface{}

	timeoutBase            int
	electionTimeout        int
	electionTimeoutCounter int
	voteHadGot             int

	nextIndex  []uint64
	matchIndex []uint64

	trustFollower map[uint64]bool

	lastIncludeIndex uint64
	lastIncludeTerm  uint64
}

func newRaft(cfg RaftCfg) *Raft {
	rf := &Raft{}

	lg, _ := zap.NewProduction()
	rf.lg = lg.Sugar()

	rf.mu = sync.RWMutex{}
	rf.nodeTotal = cfg.NodeTotal
	rf.client = MakeClerk(cfg.Peers)
	rf.Cfg = cfg
	rf.persister = cfg.Persister

	rf.role = pb.Role_Follower
	rf.me = cfg.Me

	rf.stepFunc = rf.electionTimeoutMonitor

	rf.raftLog = log.NewMemoryLog()

	rf.applyCh = cfg.ApplyCh
	rf.loopCh = make(chan *opRequest, loopChannelSize)
	rf.exitCh = make(chan interface{})

	rf.timeoutBase = cfg.ElectionTimeoutBase
	rf.electionTimeout = timeoutRand(rf.timeoutBase)

	go rf.mainLoop()

	return rf
}

func (rf *Raft) DoWork(req *opRequest) {
	rf.loopCh <- req
}

func (rf *Raft) Kill() {
	close(rf.exitCh)
}

func (rf *Raft) GetState() (uint64, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	term := rf.term
	isLeader := rf.isLeader()
	return term, isLeader
}

func (rf *Raft) GetRaftLogSize() uint64 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return 0
}

func (rf *Raft) CompressLog(lastInclude uint64, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastIncludeIndex = lastInclude
	rf.lastIncludeTerm = rf.raftLog.GetLogTerm(lastInclude)
	rf.persist()
	rf.raftLog.RemoveExpiredLogs(lastInclude)
}

func (rf *Raft) mainLoop() {
	defer func() {
		rf.lg.Infow("main loop exit", zap.Uint64("me", rf.me),
			zap.Uint64("term", rf.term))
	}()

	stepTicker := time.NewTicker(time.Millisecond * 10)
	applyTicker := time.NewTicker(time.Millisecond * 10)
	syncFollowerTicker := time.NewTicker(time.Millisecond * 200)
	for {
		select {
		case <-stepTicker.C:
			rf.stepFunc()
		case <-applyTicker.C:
			rf.applyCommand()
		case <-syncFollowerTicker.C:
			if rf.isLeader() {
				rf.syncFollower()
			}
		case req := <-rf.loopCh:
			switch req.typ {
			case requestVote:
				rf.requestVote(req)
			case appendEntries:
				rf.appendEntries(req)
			case installSnapshot:
				rf.installSnapshot(req)
			case propose:
				rf.propose(req)
			}
		case <-rf.exitCh:
			return
		}
	}
}

/*-------- gRPC interface for raft peer --------*/
func (rf *Raft) requestVote(req *opRequest) {
	args := req.args.(*pb.RequestVoteRequest)
	reply := req.reply.(*pb.RequestVoteReply)

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Reject = true
		req.errorCh <- nil
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = 0
	}
	canVote := rf.voteFor == args.Id ||
		(rf.voteFor == 0 && rf.raftLog.IsUptoDate(args.LastIndex, args.LastTerm))
	if canVote {
		rf.voteFor = args.Id
		rf.electionTimeoutCounter = 0
		rf.becomeFollower(args.Term, 0)
	} else {
		reply.Reject = true
	}
	reply.Term = rf.term
	req.errorCh <- nil
}

func (rf *Raft) appendEntries(req *opRequest) {
	args := req.args.(*pb.AppendEntriesRequest)
	reply := req.reply.(*pb.AppendEntriesReply)

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Reject = true
		req.errorCh <- nil
		return
	}

	if len(args.Entries) == 0 {
		rf.leader = args.Id
		rf.electionTimeoutCounter = 0
		rf.becomeFollower(args.Term, args.Id)
		rf.persist()

		if args.Commit > rf.commit {
			applyIndex := min(args.Commit, rf.raftLog.LastIndex())
			if rf.commit < applyIndex {
				rf.commit = applyIndex
			}
		}

		reply.Info = &pb.AppendEntriesReply_LogInfo{
			LogInfo: &pb.LogInfo{
				LatestTerm:  rf.raftLog.LastTerm(),
				LatestIndex: rf.raftLog.LastIndex(),
			},
		}
		reply.Term = rf.term
		req.errorCh <- nil
		return
	}

	rf.term = args.Term
	if rf.raftLog.Match(args.LastLogIndex, args.LastLogTerm) {
		rf.raftLog.Catoff(args.LastLogIndex + 1)
		rf.raftLog.Append(args.Entries)
		applyIndex := min(args.Commit, rf.raftLog.LastIndex())
		if rf.commit < applyIndex {
			rf.commit = applyIndex
		}
		rf.persist()
	} else {
		var matchIndex, matchTerm uint64
		matchIndex = rf.raftLog.GetMaxMatchIndex(args.LastLogTerm)
		if matchIndex == 0 {
			matchTerm = rf.raftLog.LastTerm()
		} else {
			matchTerm = args.Term
		}
		reply.Info = &pb.AppendEntriesReply_MatchInfo{MatchInfo: &pb.MatchInfo{MatchIndex: matchIndex, MatchTerm: matchTerm}}
		reply.Reject = true
	}
	reply.Term = rf.term
	req.errorCh <- nil

	return
}

func (rf *Raft) installSnapshot(req *opRequest) {
	args := req.args.(*pb.InstallSnapshotRequest)
	reply := req.args.(*pb.InstallSnapshotReply)

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Reject = true
		req.errorCh <- nil
		return
	}

	if args.Term > rf.term {
		rf.electionTimeoutCounter = 0
		rf.electionTimeout = timeoutRand(rf.timeoutBase)
		rf.becomeFollower(args.Term, args.Leader)
	}

	if rf.raftLog.Match(args.LastIncludeIndex, args.LastIncludeTerm) {
		rf.lastIncludeIndex = args.LastIncludeIndex
		rf.lastIncludeTerm = args.LastIncludeTerm
		rf.persister.SaveSnapshot(args.Data)
		rf.persist()
		rf.raftLog.RemoveExpiredLogs(args.LastIncludeIndex)
	} else {
		reply.Reject = true
	}
	reply.Term = rf.term
	req.errorCh <- nil
}

func (rf *Raft) propose(req *opRequest) {
	args := req.args.(*pb.ProposeRequest)
	reply := req.reply.(*pb.ProposeReply)

	if !rf.isLeader() {
		reply.Term = 0
		reply.Index = 0
		reply.Leader = false
		req.errorCh <- nil
		return
	}

	rf.raftLog.Append([]*pb.Entry{&pb.Entry{Term: rf.term, Index: rf.raftLog.LastIndex() + 1, Content: args.Command}})
	rf.nextIndex[rf.me] = rf.raftLog.LastIndex() + 1
	rf.matchIndex[rf.me] = rf.raftLog.LastIndex()
	rf.persist()

	var id uint64
	for id = 1; id <= rf.nodeTotal; id++ {
		if id == rf.me {
			continue
		}

		rf.doSync(id)
	}

	reply.Term = rf.term
	reply.Index = rf.raftLog.LastIndex()
	reply.Leader = true
	req.errorCh <- nil
	return
}

func (rf *Raft) formatAppendEntriesArgs(peerID uint64) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		Id:           rf.me,
		Term:         rf.term,
		Commit:       rf.commit,
		LastLogIndex: rf.nextIndex[peerID] - 1,
		LastLogTerm:  rf.raftLog.GetLogTerm(rf.nextIndex[peerID] - 1),
		Entries:      rf.raftLog.GetLogs(rf.nextIndex[peerID]),
	}
}

func (rf *Raft) formatSnapshotArgs(peerID uint64) *pb.InstallSnapshotRequest {
	return &pb.InstallSnapshotRequest{
		Id:               rf.me,
		Term:             rf.term,
		Leader:           rf.leader,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Offset:           0, // TODO snapshot 分片
		Data:             rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) handleAppendEntriesReply(reply *pb.AppendEntriesReply, id uint64) bool {
	if reply.Term > rf.term {
		rf.becomeFollower(reply.Term, 0)
		return false
	}

	handleHeartbeatReply := func(info *pb.LogInfo) bool {
		latestIndex := info.LatestIndex
		latestTerm := info.LatestTerm
		if rf.raftLog.GetLogTerm(latestIndex) == latestTerm {
			rf.nextIndex[id] = latestIndex + 1
			rf.trustFollower[id] = true
		} else {
			rf.nextIndex[id] = latestIndex
		}
		return true
	}

	handleAppendLogReply := func(reject bool, info *pb.MatchInfo) bool {
		matchIndex := info.MatchIndex
		matchTerm := info.MatchTerm
		if reject {
			if matchIndex == 0 {
				rf.nextIndex[id] = rf.raftLog.GetMaxMatchIndex(matchTerm) + 1
			} else {
				rf.nextIndex[id] = matchIndex + 1
			}
		} else {
			rf.nextIndex[id] = rf.raftLog.LastIndex() + 1
			rf.matchIndex[id] = rf.raftLog.LastIndex()
			rf.updateCommit()
			rf.trustFollower[id] = true
		}
		return true
	}

	switch reply.Info.(type) {
	case *pb.AppendEntriesReply_LogInfo:
		return handleHeartbeatReply(reply.GetLogInfo())
	case *pb.AppendEntriesReply_MatchInfo:
		return handleAppendLogReply(reply.Reject, reply.GetMatchInfo())
	default:
		return handleAppendLogReply(reply.Reject, reply.GetMatchInfo())
	}

}

func (rf *Raft) updateCommit() {
	matchArr := make([]uint64, len(rf.matchIndex))
	copy(matchArr, rf.matchIndex)
	sort.Slice(matchArr, func(i, j int) bool {
		return matchArr[i] < matchArr[j]
	})
	if matchArr[len(matchArr)/2] > rf.commit {
		rf.commit = matchArr[len(matchArr)/2]
	}
}

func (rf *Raft) applyCommand() {
	for index := rf.applied + 1; index <= rf.commit; index++ {
		msg := pb.ApplyMsg{
			Index:       index,
			Content:     rf.raftLog.GetLog(index).Content,
			UseSnapshot: false,
			Snapshot:    nil,
		}
		rf.applyCh <- msg
		rf.applied++
		rf.persist()
	}
}

func (rf *Raft) syncFollower() {
	var id uint64
	for id = 1; id <= rf.nodeTotal; id++ {
		if id == rf.me {
			continue
		}

		rf.doSync(id)
	}
}

func (rf *Raft) doSync(peerID uint64) bool {
	nextIndex := rf.nextIndex[peerID]
	if nextIndex < rf.raftLog.FirstIndex() {
		reply, err := rf.client.InstallSnapshot(context.Background(), peerID, rf.formatSnapshotArgs(peerID))
		if err != nil {
			return true
		}
		if !rf.handleSnapshotReply(reply, peerID) {
			return false
		}
	} else if rf.nextIndex[peerID] <= rf.raftLog.LastIndex() {
		reply, err := rf.client.AppendEntries(context.Background(), peerID, rf.formatAppendEntriesArgs(peerID))
		if err != nil {
			return true
		}
		if !rf.handleAppendEntriesReply(reply, peerID) {
			return false
		}
	}
	return false
}

func (rf *Raft) handleSnapshotReply(reply *pb.InstallSnapshotReply, id uint64) bool {
	if reply.Term > rf.term {
		rf.becomeFollower(reply.Term, 0)
		return false
	}

	if reply.Reject {
		return false
	} else {
		rf.nextIndex[id] = rf.lastIncludeIndex + 1
		rf.matchIndex[id] = rf.lastIncludeIndex
		rf.updateCommit()
		rf.trustFollower[id] = true
		return true
	}
}

func (rf *Raft) isLeader() bool {
	return rf.me == rf.leader
}

func (rf *Raft) electionTimeoutMonitor() {
	rf.electionTimeoutCounter++
	if rf.electionTimeoutCounter >= rf.electionTimeout {
		rf.electionTimeoutCounter = 0
		rf.becomeCandidate()
		rf.startElection()
	}
}

func (rf *Raft) startElection() {
	rf.lg.Infow("start election", zap.Uint64("me", rf.me), zap.Uint64("term", rf.term))

	var id uint64
	for id = 1; id <= rf.nodeTotal; id++ {
		if id == rf.me {
			continue
		}

		reply, err := rf.client.RequestVote(context.Background(), id, &pb.RequestVoteRequest{
			Id:        rf.me,
			Term:      rf.term,
			LastIndex: rf.raftLog.LastIndex(),
			LastTerm:  rf.raftLog.LastTerm(),
		})
		if err != nil {
			continue
		}

		if reply.Term < rf.term {
			continue
		}

		if reply.Term > rf.term {
			rf.electionTimeoutCounter = 0
			rf.becomeFollower(reply.Term, 0)
			return
		}

		if reply.Reject == false {
			rf.voteHadGot++
			if rf.voteHadGot >= int(rf.nodeTotal/2+1) {
				rf.electionTimeoutCounter = 0
				rf.becomeLeader()
				return
			}
		}
	}
	rf.electionTimeoutCounter = 0
	rf.electionTimeout = timeoutRand(rf.timeoutBase)
}

func (rf *Raft) becomeLeader() {
	rf.lg.Infow("become leader", zap.Uint64("me", rf.me),
		zap.Uint64("term", rf.term))

	rf.leader = rf.me
	rf.role = pb.Role_Leader
	rf.stepFunc = rf.leaderHeartbeat
	rf.trustFollower = make(map[uint64]bool)

	var id uint64
	rf.nextIndex = make([]uint64, rf.nodeTotal+1)
	rf.matchIndex = make([]uint64, rf.nodeTotal+1)
	for id = 1; id <= rf.nodeTotal; id++ {
		rf.nextIndex[id] = rf.raftLog.LastIndex() + 1
		if id == rf.me {
			rf.matchIndex[id] = rf.raftLog.LastIndex()
		} else {
			rf.matchIndex[id] = 0
		}
	}
	rf.persist()
	rf.leaderHeartbeat()
}

func (rf *Raft) becomeCandidate() {
	rf.term++
	rf.voteHadGot = 0
	rf.voteFor = rf.me
	rf.voteHadGot++
}

func (rf *Raft) becomeFollower(term, leader uint64) {
	rf.role = pb.Role_Follower
	rf.term = term
	rf.leader = leader
	rf.stepFunc = rf.electionTimeoutMonitor
}

func (rf *Raft) leaderHeartbeat() {
	var id uint64
	for id = 1; id <= rf.nodeTotal; id++ {
		if id == rf.me {
			continue
		}
		req := &pb.AppendEntriesRequest{
			Id:           rf.me,
			Term:         rf.term,
			LastLogIndex: rf.nextIndex[id] - 1,
			LastLogTerm:  rf.raftLog.GetLogTerm(rf.nextIndex[id] - 1),
		}
		if _, ok := rf.trustFollower[id]; ok {
			req.Commit = rf.commit
		}
		rf.client.AppendEntries(context.Background(), id, req)
	}
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(rf.term); err != nil {
		panic(err.Error())
	}
	if err := e.Encode(rf.voteFor); err != nil {
		panic(err.Error())
	}
	if err := e.Encode(rf.commit); err != nil {
		panic(err.Error())
	}
	if err := e.Encode(rf.lastIncludeIndex); err != nil {
		panic(err.Error())
	}
	if err := e.Encode(rf.lastIncludeTerm); err != nil {
		panic(err.Error())
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		if err := d.Decode(&rf.term); err != nil {
			panic(err.Error())
		}
		if err := d.Decode(&rf.voteFor); err != nil {
			panic(err.Error())
		}
		if err := d.Decode(&rf.commit); err != nil {
			panic(err.Error())
		}
		if err := d.Decode(&rf.lastIncludeIndex); err != nil {
			panic(err.Error())
		}
		if err := d.Decode(&rf.lastIncludeTerm); err != nil {
			panic(err.Error())
		}
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func timeoutRand(base int) int {
	return 10 + int(rand.NewSource(time.Now().UnixNano()).Int63()%int64(base))
}
