package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0
const TIMEOUT = time.Millisecond * 100

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OPGET    = 0
	OPPUT    = 1
	OPAPPEND = 2
)

type Op struct {
	Type   int
	Key    string
	Value  string
	Client int64
	Id     int64
}

type PendingOp struct {
	flag chan bool
	op   *Op
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int
	persister    *raft.Persister
	data         map[string]string
	pendingOps   map[int][]*PendingOp
	opCount      map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	var op Op
	op.Type = OPGET
	op.Key = args.Key
	op.Value = args.Value
	op.Client = args.Client
	op.Id = args.Id
	reply.WrongLeader = kv.exec(op)
	if reply.WrongLeader {
		reply.Err = ErrNotLeader
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if value, ok := kv.data[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var op Op
	if args.Op == "Put" {
		op.Type = OPPUT
	} else {
		op.Type = OPAPPEND
	}
	op.Key = args.Key
	op.Value = args.Value
	op.Client = args.Client
	op.Id = args.Id
	reply.WrongLeader = kv.exec(op)
	if reply.WrongLeader {
		reply.Err = ErrNotLeader
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) exec(op Op) bool {
	index, _, flag := kv.rf.Start(op)
	if !flag {
		return true
	}

	waiter := make(chan bool, 1)
	kv.mu.Lock()
	kv.pendingOps[index] = append(kv.pendingOps[index], &PendingOp{flag: waiter, op: &op})
	kv.mu.Unlock()

	var ok bool
	timer := time.NewTimer(TIMEOUT)

	select {
	case ok = <-waiter:
	case <-timer.C:
		ok = false
	}
	kv.mu.Lock()
	delete(kv.pendingOps, index)
	kv.mu.Unlock()
	return !ok
}

func (kv *KVServer) Apply(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	args := msg.Command.(Op)
	if args.Id > kv.opCount[args.Client] {
		switch args.Type {
		case OPPUT:
			kv.data[args.Key] = args.Value
		case OPAPPEND:
			kv.data[args.Key] = kv.data[args.Key] + args.Value
		default:
		}
		kv.opCount[args.Client] = args.Id
	}
	for _, i := range kv.pendingOps[msg.CommandIndex] {
		if i.op.Client == args.Client && i.op.Id == args.Id {
			i.flag <- true
		} else {
			i.flag <- false
		}
	}
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.pendingOps = make(map[int][]*PendingOp)
	kv.opCount = make(map[int64]int64)

	go func() {
		for msg := range kv.applyCh {
			kv.Apply(&msg)
		}
	}()

	return kv
}
