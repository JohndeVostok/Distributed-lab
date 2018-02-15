package raft

import (
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
		}
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
	}
	rf.resetTimer()
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.state = "FOLLOWER"
	rf.applyChan = applyCh
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.resetTimer()

	return rf
}
