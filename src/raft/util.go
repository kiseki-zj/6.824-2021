package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(time.Millisecond * 150)
}

func RandomizedElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

func (rf *Raft) ChangeState(state NodeState) {
	rf.state = state
	if state == StateFollower {
		rf.electionTimer.Reset(RandomizedElectionTimeout())
	}
	if state == StateLeader {
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.getLastLog().Idx + 1
		}
		rf.matchIndex[rf.me] = rf.getLastLog().Idx
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
	rf.persist()
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs[len(rf.logs)-1].Idx,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	return req
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getFirstLog() *Entry {
	return &rf.logs[0]
}

func (rf *Raft) getLastLog() *Entry {
	return &rf.logs[len(rf.logs)-1]
}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesArgs {

	request := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.getLogTermWithIndex(prevLogIndex),
		LeaderCommit: rf.commitIndex,
	}
	DPrintf("[genAppendEntries]prevlogIdx=%v, loglen=%v, getlastlog.idx=%v", prevLogIndex, len(rf.logs), rf.getLastLog().Idx)
	if prevLogIndex == rf.getLastLog().Idx {
		//heartbeat
		request.Entries = []Entry{}
	} else {
		entries := make([]Entry, 0)
		entries = append(entries, rf.logs[prevLogIndex-rf.getFirstLog().Idx+1:]...)
		request.Entries = entries
	}
	return request
}

func (rf *Raft) getLogTermWithIndex(globalIndex int) int {
	return rf.logs[globalIndex-rf.getFirstLog().Idx].Term
}

func (rf *Raft) matchLog(prevlogterm, prevlogindex int) bool {
	return rf.getLogTermWithIndex(prevlogindex) == prevlogterm
}

func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastIndex := rf.getLastLog().Idx
	lastTerm := rf.getLastLog().Term
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == StateLeader && rf.matchIndex[peer] < rf.getLastLog().Idx
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
