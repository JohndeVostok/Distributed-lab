package raft

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

const (
	HeartbeatCycle  = time.Millisecond * 50
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int

	currentTerm int
	votedFor    int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	vote      int
	state     string
	applyChan chan ApplyMsg
	timer     *time.Timer
}

type AppendEntryArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term        int
	Success     bool
	CommitIndex int
}

type RequestVoteArgs struct {
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.logs)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)
	decoder.Decode(&rf.currentTerm)
	decoder.Decode(&rf.votedFor)
	decoder.Decode(&rf.logs)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	flag := true
	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
			flag = false
		}
		if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
			flag = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm {
		if flag && rf.votedFor == -1 {
			rf.votedFor = args.Candidate
			rf.persist()
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.Candidate)
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if flag {
			rf.votedFor = args.Candidate
			rf.persist()
		}
		rf.resetTimer()
		reply.Term = rf.currentTerm
		reply.VoteGranted = (rf.votedFor == args.Candidate)
		return
	}
}

func (rf *Raft) handleVoteReply(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}
	if rf.state == CANDIDATE && reply.VoteGranted {
		rf.vote += 1
		if rf.vote >= len(rf.peers)/2+1 {
			rf.state = LEADER
			rf.resetTimer()
		}
		return
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) resetTimer() {
	if rf.timer == nil {
		rf.timer = time.NewTimer(time.Millisecond * 1000)
		go func() {
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}

	timeout := HeartbeatCycle
	if rf.state != LEADER {
		timeout = time.Millisecond * time.Duration(ElectionMinTime+rand.Intn(ElectionMaxTime-ElectionMinTime))
	}
	rf.timer.Reset(timeout)
}

func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.vote = 1
		rf.persist()
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			Candidate:    rf.me,
			LastLogIndex: len(rf.logs) - 1,
		}
		if len(rf.logs) > 0 {
			args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		for i, _ := range rf.peers {
			if i != rf.me {
				go func(server int, args RequestVoteArgs) {
					var reply RequestVoteReply
					ok := rf.sendRequestVote(server, &args, &reply)
					if ok {
						rf.handleVoteReply(reply)
					}
				}(i, args)
			}
		}
	} else {
		rf.SendAppendEntries()
	}
	rf.resetTimer()
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = args.Term

		if args.PrevLogIndex >= 0 && (len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.CommitIndex = len(rf.logs) - 1
			if reply.CommitIndex > args.PrevLogIndex {
				reply.CommitIndex = args.PrevLogIndex
			}
			for reply.CommitIndex >= 0 {
				if rf.logs[reply.CommitIndex].Term == args.PrevLogTerm {
					break
				}
				reply.CommitIndex--
			}
			reply.Success = false
		} else if args.Entries != nil {
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			rf.logs = append(rf.logs, args.Entries...)
			if len(rf.logs) > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLogs()
			}
			reply.CommitIndex = len(rf.logs) - 1
			reply.Success = true
		} else {
			if len(rf.logs) > args.LeaderCommit {
				rf.commitIndex = args.LeaderCommit
				go rf.commitLogs()
			}
			reply.CommitIndex = args.PrevLogIndex
			reply.Success = true
		}
	}
	rf.persist()
	rf.resetTimer()
}

func (rf *Raft) handleAppendEntry(server int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.resetTimer()
		return
	}

	if reply.Success {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.matchIndex[server] = reply.CommitIndex
		nMatched := 1
		for i, _ := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= rf.matchIndex[server] {
				nMatched++
			}
		}
		if nMatched > len(rf.peers)/2 && rf.commitIndex < rf.matchIndex[server] && rf.logs[rf.matchIndex[server]].Term == rf.currentTerm {
			rf.commitIndex = rf.matchIndex[server]
			go rf.commitLogs()
		}
	} else {
		rf.nextIndex[server] = reply.CommitIndex + 1
		rf.SendAppendEntries()
	}
}

func (rf *Raft) SendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntries() {
	for i, _ := range rf.peers {
		if i != rf.me {
			var args AppendEntryArgs
			args.Term = rf.currentTerm
			args.Leader = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			}
			if rf.nextIndex[i] < len(rf.logs) {
				args.Entries = rf.logs[rf.nextIndex[i]:]
			}
			args.LeaderCommit = rf.commitIndex

			go func(server int, args AppendEntryArgs) {
				var reply AppendEntryReply
				ok := rf.SendAppendEntry(server, &args, &reply)
				if ok {
					rf.handleAppendEntry(server, reply)
				}
			}(i, args)
		}
	}
}

func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex >= len(rf.logs) {
		rf.commitIndex = len(rf.logs) - 1
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{CommandValid: true, CommandIndex: i + 1, Command: rf.logs[i].Command}
		rf.applyChan <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	if rf.state == LEADER {
		isLeader = true
		log := LogEntry{command, rf.currentTerm}
		rf.logs = append(rf.logs, log)
		index = len(rf.logs)
		term = rf.currentTerm
	}
	rf.persist()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = FOLLOWER
	rf.applyChan = applyCh
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.resetTimer()

	return rf
}
