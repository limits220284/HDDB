package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	seq      int
	leader   int
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
	ck.clientId = int(nrand())
	ck.seq = 0
	DPrintf("MakeClerk %v", ck)
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("[%v]Client Get %+v", ck.clientId, ck)
	ck.seq += 1
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		Seq:      ck.seq,
	}
	for {
		reply := GetReply{}
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				ck.changeLeader()
			case ErrTimeout:
				continue
			default:
				continue
			}
		} else {
			ck.changeLeader()
		}
		time.Sleep(ChangeLeaderPeriods)
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	ck.seq += 1
	args := PutAppendArgs{
		ClientId: ck.clientId,
		Seq:      ck.seq,
		Key:      key,
		Value:    value,
		Op:       op,
	}

	for {
		reply := &PutAppendReply{}
		DPrintf("[%v]->[%v]Client PutAppend %+v, %+v", ck.clientId, ck.leader, args, ck)
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.changeLeader()
			case ErrTimeout:
				continue
			default:
				continue
			}
		} else {
			ck.changeLeader()
		}
		time.Sleep(ChangeLeaderPeriods)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) changeLeader() {
	ck.leader += 1
	if ck.leader >= len(ck.servers) {
		ck.leader = 0
	}
}
