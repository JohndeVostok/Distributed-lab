package raftkv

const (
	OK           = "OK"
	ErrNoKey     = "ErrNoKey"
	ErrNotLeader = "ErrNotLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Op     string
	Client int64
	Id     int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key    string
	Value  string
	Client int64
	Id     int64
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
