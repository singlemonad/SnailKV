package log

import (
	pb "github.com/singlemonad/SnailKV/proto"
)

type RaftLog interface {
	FirstIndex() uint64
	LastTerm() uint64
	LastIndex() uint64
	Match(logIndex, logTerm uint64) bool
	IsUptoDate(lastIndex, lastTerm uint64) bool
	GetLogTerm(index uint64) uint64
	GetLog(index uint64) *pb.Entry
	GetLogs(startIndex uint64) []*pb.Entry
	Append(entries []*pb.Entry)
	GetMaxMatchIndex(term uint64) uint64
	Catoff(index uint64)
	RemoveExpiredLogs(endIndex uint64)
}

// MemoryLog is raft log in memory, not persistent to disk
// entries[0] not use, log start at entries[1]
type MemoryLog struct {
	len     int
	entries []*pb.Entry
}

func NewMemoryLog() RaftLog {
	l := &MemoryLog{
		entries: make([]*pb.Entry, 1),
	}
	l.entries[0] = &pb.Entry{}
	l.len = 0
	return l
}

func (l *MemoryLog) FirstIndex() uint64 {
	return l.entries[0].Index
}

func (l *MemoryLog) LastTerm() uint64 {
	return l.entries[l.len].Term
}

func (l *MemoryLog) LastIndex() uint64 {
	return uint64(l.len)
}

func (l *MemoryLog) Match(logIndex, logTerm uint64) bool {
	if logIndex > uint64(l.len) {
		return false
	}
	return l.entries[logIndex].Term == logTerm
}

func (l *MemoryLog) IsUptoDate(lastIndex, lastTerm uint64) bool {
	if lastTerm > l.LastTerm() {
		return true
	} else if lastTerm < l.LastTerm() {
		return false
	} else {
		if lastIndex >= l.LastIndex() {
			return true
		}
		return false
	}
}

func (l *MemoryLog) GetLogTerm(index uint64) uint64 {
	return l.entries[index].Term
}

func (l *MemoryLog) GetLog(index uint64) *pb.Entry {
	if index > uint64(l.len) {
		return nil
	}
	return l.entries[index]
}

func (l *MemoryLog) GetLogs(startIndex uint64) []*pb.Entry {
	if startIndex > uint64(l.len) {
		return nil
	}
	return l.entries[startIndex : l.len+1]
}

func (l *MemoryLog) Append(entries []*pb.Entry) {
	l.entries = append(l.entries, entries...)
	l.len += len(entries)
}

func (l *MemoryLog) GetMaxMatchIndex(term uint64) uint64 {
	for index := l.len; index >= 0; index-- {
		if l.entries[index].Term == term {
			return uint64(index)
		}

		if l.entries[index].Term < term {
			return 0
		}
	}
	return 0
}

func (l *MemoryLog) Catoff(index uint64) {
	l.entries = l.entries[1:index]
	l.len = len(l.entries) - 1
}

func (l *MemoryLog) RemoveExpiredLogs(endIndex uint64) {
	l.entries = l.entries[endIndex+1 : len(l.entries)]
	l.len = len(l.entries) - 1
}
