package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	leader  int
	client  int64
	stamp   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.client = nrand()
	ck.leader = 0
	ck.stamp = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	var args GetArgs
	args.Key = key
	ck.mu.Lock()
	args.Client = ck.client
	ck.stamp++
	args.Id = ck.stamp
	leader := ck.leader
	ck.mu.Unlock()
	for {
		var reply GetReply
		ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if reply.Err == OK {
			return reply.Value
		} else {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			leader = ck.leader
			ck.mu.Unlock()
		}
	}
	return ""
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	ck.mu.Lock()
	args.Client = ck.client
	ck.stamp++
	args.Id = ck.stamp
	leader := ck.leader
	ck.mu.Unlock()
	for {
		var reply PutAppendReply
		ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == OK {
			break
		} else {
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			leader = ck.leader
			ck.mu.Unlock()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
