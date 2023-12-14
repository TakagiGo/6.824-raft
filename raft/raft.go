package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Rfstatus int

const (
	leader Rfstatus = iota
	follower
	candidate
)

type EventType int

const (
	TickerTimeOutEvent EventType = iota
	VoteEvent
	ApplyLogEvent
)

type Event struct {
	eventType EventType
	eventData interface{}
}

type Log struct {
	LeaderTerm int
	Command    interface{}
}

func _min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	peersNumber int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//for all server
	logs         []*Log
	LastlogIndex int
	currentTerm  int
	votedFor     int
	commitIndex  int
	lastApplied  int

	rfstatus     Rfstatus
	processCh    chan *Event
	applyCh      chan ApplyMsg
	lastTickTime time.Time

	//snapShort
	lastIncludedIndex int
	lastIncludedItem  int
	snapShort         []byte

	//for leader
	HeartBeatGap          int
	VoteCollection        int
	nextIndex             []int
	matchIndex            []int
	lastAppendEntriesArgs []*AppendEntriesArgs
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.rfstatus == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	//Debug(dPersist, "s%d persist the log,length is %d", rf.me, len(raftstate))
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	Debug(dLog, "s%d readPersist persist length is %d", rf.me, len(data))
	if data == nil || len(data) < 1 {
		Debug(dLog, "333 readfailed") // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []*Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		Debug(dError, "S%d readPersist Failed", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		Debug(dWarn, "S%d currentTerm is %d", rf.me, currentTerm)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastIncludedItem = rf.logs[index-rf.lastIncludedIndex].LeaderTerm
	Debug(dSnap, "S%d receive a SnapshotCommand index=%d", rf.me, index)
	rf.logs = append(rf.logs[0:1], rf.logs[index-rf.lastIncludedIndex+1:]...)
	rf.lastIncludedIndex = index
	Debug(dSnap, "S%d update lastIncludedItem=%d lastIncludedIndex=%d,length is %d", rf.me, rf.lastIncludedItem, rf.lastIncludedIndex, len(rf.logs))
	rf.snapShort = snapshot
	for i := 0; i < rf.peersNumber; i++ {
		rf.nextIndex[i] = rf.LastlogIndex + 1
	}
	rf.persist()
}

type AppendEntriesArgs struct {
	CurrentTerm  int
	LeaderId     int
	PrevLogIndex int
	PrevLogItem  int
	Entries      []*Log
	LeaderCommit int
	AppendType   AppendType
	LatestIndex  int
	// Your data here (2A, 2B).
}

type AppendType int

const (
	AppendHeartBeat AppendType = iota
	AppendLog
)

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	CurrentTerm    int
	Id             int
	Success        bool
	AppendType     AppendType
	ApplyIndex     int
	FailFirstIndex int
	IsOutOFdate    bool
	LastLogItem    int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogItem  int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	ReplyId     int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedItem  int
	SnapShot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) UpdateTerm(term int) {
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) UpdateVoteFor(voteFor int) {
	rf.votedFor = voteFor
	rf.persist()
}

func (rf *Raft) UpdateLogs(Logs []*Log) {
	rf.logs = Logs
	rf.persist()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		Debug(dInfo, "S%d receive a out of date Vote Request from %d, reject it", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.ReplyId = rf.me
		reply.VoteGranted = false
	} else if rf.votedFor != -1 && rf.currentTerm == args.Term {
		Debug(dInfo, "S%d receive a Vote Request from %d,but already vote for %d", rf.me, args.CandidateId, rf.votedFor)
		reply.Term = rf.currentTerm
		reply.ReplyId = rf.me
		reply.VoteGranted = false
	} else if rf.logs[len(rf.logs)-1].LeaderTerm > args.LastLogItem || (rf.LastlogIndex > args.LastLogIndex && rf.logs[len(rf.logs)-1].LeaderTerm == args.LastLogItem) {
		Debug(dInfo, "S%d receive a Vote Request from %d,but LogIndex leader=%d < me=%d || LeaderTerm leader=%d < me=%d", rf.me, args.CandidateId, args.LastLogIndex, rf.LastlogIndex, args.LastLogItem, rf.logs[len(rf.logs)-1].LeaderTerm)
		reply.Term = rf.currentTerm
		reply.ReplyId = rf.me
		reply.VoteGranted = false
	} else {
		//Debug(dInfo, "S%d receive a Vote Request from %d", rf.me, args.CandidateId)
		Debug(dInfo, "S%d receive a Vote Request from %d LogIndex leader=%d  me=%d LeaderTerm leader=%d  me=%d", rf.me, args.CandidateId, args.LastLogIndex, rf.LastlogIndex, args.LastLogItem, rf.logs[len(rf.logs)-1].LeaderTerm)
		rf.lastTickTime = time.Now()
		reply.Term = args.Term
		reply.ReplyId = rf.me
		rf.UpdateVoteFor(args.CandidateId)
		//rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		if rf.rfstatus != follower {
			Debug(dInfo, "S%d receive a Vote Request from %d, term is out of date", rf.me, args.CandidateId)
			rf.lastTickTime = time.Now()
			rf.rfstatus = follower
		}
		//todo 2B
	}
	if rf.currentTerm < args.Term {
		//rf.currentTerm = args.Term
		rf.UpdateTerm(args.Term)
		if rf.rfstatus != follower {
			Debug(dInfo, "S%d receive a Vote Request from %d, term is out of date", rf.me, args.CandidateId)
			rf.lastTickTime = time.Now()
			rf.rfstatus = follower
		}
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == false {
		if args.AppendType == AppendHeartBeat {
			if args.CurrentTerm >= rf.currentTerm {
				rf.lastTickTime = time.Now()
				Debug(dLog2, "S%d receive a heartbeat from leader %d,term is %d", rf.me, args.LeaderId, args.CurrentTerm)
				//rf.currentTerm = args.CurrentTerm
				rf.UpdateTerm(args.CurrentTerm)
				if rf.rfstatus != follower {
					rf.lastTickTime = time.Now()
					rf.rfstatus = follower
				}
			} else {
				Debug(dError, "S%d receive a invalid heartbeat from leader %d,my_term: %d,args_term: %d", rf.me, args.LeaderId, rf.currentTerm, args.CurrentTerm)
			}
		} else {
			if args.CurrentTerm > rf.currentTerm {
				rf.lastTickTime = time.Now()
				Debug(dLog2, "S%d receive a RPC,my_term: %d,args_term: %d", rf.me, args.LeaderId, args.CurrentTerm)
				//rf.currentTerm = args.CurrentTerm
				rf.UpdateTerm(args.CurrentTerm)
				if rf.rfstatus != follower {
					rf.lastTickTime = time.Now()
					rf.rfstatus = follower
					rf.votedFor = -1
				}
			}
			if rf.lastAppendEntriesArgs[rf.me] != nil && rf.lastAppendEntriesArgs[rf.me].PrevLogItem == args.PrevLogItem && rf.lastAppendEntriesArgs[rf.me].PrevLogIndex == args.PrevLogIndex {
				if !(rf.lastAppendEntriesArgs[rf.me].LeaderCommit < args.LeaderCommit || rf.lastAppendEntriesArgs[rf.me].LatestIndex < args.LatestIndex) {
					Debug(dLog2, "s%d receive out of date broadcastAppendLogs,PrevLogItem:%d PrevLogIndex:%d LatestIndex:%d", rf.me, args.PrevLogItem, args.PrevLogIndex, rf.lastAppendEntriesArgs[rf.me].LatestIndex)
					reply.IsOutOFdate = true
					return
				}
			}
			reply.IsOutOFdate = false
			reply.AppendType = AppendLog
			reply.Id = rf.me
			reply.CurrentTerm = rf.currentTerm
			Debug(dClient, "S%d receive a AppendLog command from leader %d,PrevLogItem=%d PrevLogIndex=%d,size=%d,leader's commitIndex=%d", rf.me, args.LeaderId, args.PrevLogItem, args.PrevLogIndex, len(args.Entries), args.LeaderCommit)
			if args.CurrentTerm < rf.currentTerm {
				reply.FailFirstIndex = -1
				reply.Success = false
				reply.LastLogItem = rf.currentTerm
				Debug(dClient, "S%d reject the AppendLog command because leader term is %d,local term is %d",
					rf.me, args.CurrentTerm, rf.currentTerm)
				Debug(dClient, "111reply.FailFirstIndex is %d", reply.FailFirstIndex)
			} else if args.PrevLogIndex > rf.LastlogIndex {
				reply.Success = false
				reply.FailFirstIndex = rf.LastlogIndex + 1
				Debug(dClient, "S%d reject the AppendLog command because leader PrevLogIndex %d, rf.LastlogIndex %d", rf.me, args.PrevLogIndex, rf.LastlogIndex)
				Debug(dClient, "222reply.FailFirstIndex is %d", reply.FailFirstIndex)
				//0 1 2 3 4 [5 6 7 8 9 10]
			} else if args.PrevLogIndex-rf.lastIncludedIndex > 0 && rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].LeaderTerm != args.PrevLogItem {
				reply.LastLogItem = rf.logs[len(rf.logs)-1].LeaderTerm
				reply.Success = false
				reply.FailFirstIndex = -1
				Debug(dClient, "S%d reject the AppendLog command because leader prevLogIndex is %d prevLogItem is %d"+
					",local LogIndex is %d,prevLogItem is %d",
					rf.me, args.PrevLogIndex, args.PrevLogItem, rf.LastlogIndex, rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].LeaderTerm)
				Debug(dClient, "333reply.FailFirstIndex is %d", reply.FailFirstIndex)
			} else if args.PrevLogIndex-rf.lastIncludedIndex < 0 {
				if args.PrevLogIndex+len(args.Entries) < rf.lastIncludedIndex {
					reply.LastLogItem = rf.currentTerm
					reply.Success = true
					reply.ApplyIndex = args.PrevLogIndex + len(args.Entries)
					return
				}
				reply.LastLogItem = rf.currentTerm
				reply.Success = true
				reply.ApplyIndex = rf.lastIncludedIndex

				return
			} else {
				Debug(dClient, "S%d rf.logs length is %d", rf.me, len(rf.logs))
				Debug(dClient, "S%d rf.Entries length is %d", rf.me, len(args.Entries))
				rf.lastAppendEntriesArgs[rf.me] = args

				rf.logs = rf.logs[0 : args.PrevLogIndex+1-rf.lastIncludedIndex]
				rf.logs = append(rf.logs, args.Entries...)

				rf.UpdateLogs(rf.logs)
				for i := args.PrevLogIndex + 1 - rf.lastIncludedIndex; i < len(rf.logs); i++ {
					Debug(dClient, "S%d append log %v Index=%d from leader %d", rf.me, rf.logs[i].Command, i, args.LeaderId)
				}
				//rf.LastlogIndex = len(rf.logs) - 1
				// 0 1 2 3 4 [5 6 7 8 9]
				rf.LastlogIndex = rf.lastIncludedIndex + len(rf.logs) - 1
				Debug(dClient, "S%d agree the AppendLog,current log length is %d", rf.me, rf.LastlogIndex)
				reply.LastLogItem = rf.currentTerm
				reply.Success = true
				reply.ApplyIndex = rf.LastlogIndex
				rf.commitIndex = _min(args.LeaderCommit, rf.LastlogIndex)
			}
		}
	}
	// Your code here (2A, 2B).
}

// 0 4 5 6 7 8

func (rf *Raft) sliceLogs(from int, to int) {
	if from-rf.lastIncludedIndex < 1 || to > rf.LastlogIndex || to < rf.lastIncludedIndex+1 {
		Debug(dDrop, "S%d drop logs failed.target=[%d,%d],me is [%d,%d]", rf.me, from, to, rf.lastIncludedIndex+1, rf.LastlogIndex)
		return
	}
	if from > rf.lastIncludedIndex {
		rf.logs = rf.logs[0:1]
		rf.LastlogIndex = from - 1
		return
	}
	Debug(dDrop, "S%d drop logs success.target= [%d,%d],me is [%d,%d]", rf.me, from, to, rf.lastIncludedIndex+1, rf.LastlogIndex)
	rf.logs = append(rf.logs[0:1], rf.logs[from-rf.lastIncludedIndex:to+1]...)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		Debug(dSnap, "S%d receive a invalid InstallSnapshot from leader%d. currentTerm:me=%d leader=%d", rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		rf.UpdateTerm(args.Term)
		if rf.rfstatus != follower {
			rf.lastTickTime = time.Now()
			rf.rfstatus = follower
		}
	}
	Debug(dSnap, "S%d receive a InstallSnapshot from leader%d term=%d.lastIncludedIndex=%d,lastIncludedItem=%d", rf.me, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedItem)
	rf.sliceLogs(args.LastIncludedIndex+1, rf.LastlogIndex)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedItem = args.LastIncludedItem
	rf.LastlogIndex = rf.lastIncludedIndex + len(rf.logs) - 1
	if args.LastIncludedIndex > rf.commitIndex {
		Debug(dSnap, "S%d commitIndex%d is out of date, update to the %d and send ApplyMsg to applych", rf.me, rf.commitIndex, args.LastIncludedIndex)
		rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.SnapShot, SnapshotIndex: rf.lastIncludedIndex, SnapshotTerm: rf.lastIncludedItem}
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = rf.commitIndex
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.rfstatus == leader {
		// Your code here (2B).
		rf.logs = append(rf.logs, &Log{LeaderTerm: rf.currentTerm, Command: command})
		//rf.UpdateLogs(append(rf.logs, &Log{LeaderTerm: rf.currentTerm, Command: command}))
		rf.UpdateLogs(rf.logs)
		rf.LastlogIndex++
		Debug(dLog, "S%d receive a command,index = %d, currentTerm = %d", rf.me, rf.LastlogIndex, rf.currentTerm)
		return rf.LastlogIndex, rf.currentTerm, true
	}
	return index, rf.currentTerm, false
}

const TickInterval int64 = 30

func (rf *Raft) appendLogToCh() {
	for !rf.killed() {
		rf.mu.Lock()
		applyMsgs := []ApplyMsg{}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsgs = append(applyMsgs, ApplyMsg{CommandValid: true, CommandIndex: rf.lastApplied, Command: rf.logs[rf.lastApplied-rf.lastIncludedIndex].Command})
		}
		rf.mu.Unlock()
		if applyMsgs != nil {
			for _, msg := range applyMsgs {
				Debug(dCommit, "S%d commit the log %v: Index=%d", rf.me, msg.Command, msg.CommandIndex)
				rf.applyCh <- msg
			}
		}
		time.Sleep(time.Duration(TickInterval) * time.Millisecond)
	}
}

func (rf *Raft) broadcastAppendLogs() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.rfstatus != leader {
			rf.mu.Unlock()
			break
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{CurrentTerm: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1, AppendType: AppendLog, LeaderCommit: rf.commitIndex}
			Debug(dLeader, "S%d send to %d PrevLogIndex=%d", rf.me, i, args.PrevLogIndex)
			args.PrevLogItem = rf.logs[args.PrevLogIndex-rf.lastIncludedIndex].LeaderTerm
			args.Entries = rf.logs[args.PrevLogIndex+1-rf.lastIncludedIndex:]
			args.LatestIndex = rf.LastlogIndex
			if rf.lastAppendEntriesArgs[i] != nil && rf.lastAppendEntriesArgs[i].PrevLogIndex == args.PrevLogIndex && rf.lastAppendEntriesArgs[i].LeaderCommit == args.LeaderCommit && len(rf.lastAppendEntriesArgs[i].Entries) == len(args.Entries) && rf.lastAppendEntriesArgs[i].LatestIndex == args.LatestIndex {
				Debug(dLog2, "s%d send equal broadcastAppendLogs to %d", rf.me, i)
				continue
			}
			rf.lastAppendEntriesArgs[i] = args
			Debug(dLeader, "S%d term%d send AppendEntries to %d: PrevLogIndex=%d, PrevLogItem=%d, entriessize=%d ", rf.me, rf.currentTerm, i, args.PrevLogIndex, args.PrevLogItem, len(args.Entries))
			go func(i int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, args, reply)
				if !ok {
					rf.lastAppendEntriesArgs[i] = nil
					Debug(dError, "S%d send AppendEntries to %d failed", rf.me, i)
				} else {
					rf.ProcessAppendLogReply(reply)
				}
			}(i, args)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(rf.HeartBeatGap) * time.Millisecond)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.mu.Lock()
	atomic.StoreInt32(&rf.dead, 1)
	Debug(dError, "S%d is be killed", rf.me)
	rf.mu.Unlock()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ProcessAppendLogReply(reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() == false {
		if rf.rfstatus != leader {
			return
		}
		if reply.CurrentTerm < rf.currentTerm || reply.IsOutOFdate {
			Debug(dLeader, "S%d receive a out of date RPC", rf.me)
			return
		}
		if reply.Success == false {
			if reply.CurrentTerm > rf.currentTerm {
				Debug(dLeader, "S%d leader term %d is out of date,newer term is %d", rf.me, rf.currentTerm, reply.CurrentTerm)
				rf.rfstatus = follower
				rf.UpdateTerm(reply.CurrentTerm)
				//rf.currentTerm = reply.CurrentTerm
				return
			}
			flag := false
			if reply.FailFirstIndex != -1 {
				if reply.FailFirstIndex > rf.lastIncludedIndex+1 {
					rf.nextIndex[reply.Id] = reply.FailFirstIndex
					flag = true
				}
			} else {
				for index := 1; index < len(rf.logs); index++ {
					if rf.logs[index].LeaderTerm == reply.LastLogItem {
						rf.nextIndex[reply.Id] = index + 1 + rf.lastIncludedIndex
						flag = true
						break
					}
				}
			}
			if !flag {
				rf.nextIndex[reply.Id] = 1 + rf.lastIncludedIndex
				if rf.snapShort != nil {
					args := &InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me, LastIncludedIndex: rf.lastIncludedIndex, LastIncludedItem: rf.lastIncludedItem, SnapShot: rf.snapShort}
					Debug(dLeader, "S%d term%d send snapShort to %d ", rf.me, rf.currentTerm, reply.Id)
					go func(i int, args *InstallSnapshotArgs) {
						reply := &InstallSnapshotReply{}
						ok := rf.sendInstallSnapshot(i, args, reply)
						if !ok {
							rf.lastAppendEntriesArgs[i] = nil
							Debug(dError, "S%d send AppendEntries to %d failed", rf.me, i)
						} else {
							//todo [2D]fix
						}
					}(reply.Id, args)
				}
			}
			Debug(dLeader, "S%d leader rf.nextIndex[%d]. nextIndex = %d", rf.me, reply.Id, rf.nextIndex[reply.Id])
		} else {
			Debug(dLeader, "S%d leader term %d receive a AppendLogReply:term%d from %d Index= %d", rf.me, rf.currentTerm, reply.CurrentTerm, reply.Id, reply.ApplyIndex)
			rf.nextIndex[reply.Id] = reply.ApplyIndex + 1
			rf.matchIndex[reply.Id] = reply.ApplyIndex
			flag := false
			to := rf.commitIndex
			for i := rf.LastlogIndex; i > rf.commitIndex && rf.logs[i-rf.lastIncludedIndex].LeaderTerm == rf.currentTerm; i-- {
				argeementNumber := 1
				for j := 0; j < rf.peersNumber; j++ {
					if j == rf.me {
						continue
					}
					if rf.matchIndex[j] >= i {
						argeementNumber++
					}
				}
				if argeementNumber > rf.peersNumber/2 {
					to = i
					flag = true
					break
				}
				//Debug(dTrace, "S%d leader term %d receive a AppendLogReply from %d Index= %d", rf.me, rf.currentTerm, reply.Id, reply.ApplyIndex)
			}
			if flag {
				for i := rf.commitIndex + 1; i <= to; i++ {
					rf.commitIndex++
					//Debug(dCommit, "S%d commit the log: Index=%d,current number is %d", rf.me, rf.commitIndex, len(rf.ApplyNumber[i]))
					//rf.applyCh <- ApplyMsg{CommandValid: true, CommandIndex: rf.commitIndex, Command: rf.logs[rf.commitIndex].Command}
					//Debug(dTrace, "S%d leader term %d receive a AppendLogReply from %d Index= %d", rf.me, rf.currentTerm, reply.Id, reply.ApplyIndex)
				}
			}
		}
	}
}

func (rf *Raft) BrocastHeartBeat() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		select {
		default:
			// Your code here (2A)
			if rf.rfstatus != leader {
				return
			}
			Debug(dLog2, "S%d Broadcast the Heart Beat", rf.me)
			// Check if a leader election should be started.
			rf.lastTickTime = time.Now()
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int) {
					args := &AppendEntriesArgs{CurrentTerm: rf.currentTerm, LeaderId: rf.me, AppendType: AppendHeartBeat}
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, args, reply)
					if !ok {
						Debug(dError, "S%d Brocast Heart Beat to %d failed", rf.me, i)
					}
				}(i)
			}
		}
		time.Sleep(time.Duration(rf.HeartBeatGap) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = -1
	rf.rfstatus = candidate
	rf.VoteCollection = 0
	rf.currentTerm++
	rf.lastTickTime = time.Now()
	//rf.votedFor = rf.me
	rf.UpdateVoteFor(rf.me)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.LastlogIndex,
		LastLogItem:  rf.logs[rf.LastlogIndex-rf.lastIncludedIndex].LeaderTerm,
		//todo (2B)
	}
	eventData := &RequestVoteReply{Term: rf.currentTerm, VoteGranted: true, ReplyId: rf.me}
	go rf.startVote(eventData)
	Debug(dVote, "S%d broadcast the vote %v", rf.me, args)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, reply)
			if !ok {
				Debug(dError, "S%d send request vote to %d failed", rf.me, i)
			} else {
				rf.startVote(reply)
			}
		}(i, args)
	}
}

func (rf *Raft) startVote(reply *RequestVoteReply) {
	if rf.rfstatus != candidate {
		return
	}
	if reply.VoteGranted == false {
		return
	}
	if reply.Term != rf.currentTerm {
		return
	}
	rf.VoteCollection++
	Debug(dVote, "S%d receive a voteGrant from %d,now VoteNumber is %d", rf.me, reply.ReplyId, rf.VoteCollection)
	if rf.VoteCollection > rf.peersNumber/2 {
		rf.mu.Lock()
		rf.rfstatus = leader
		for i := 0; i < rf.peersNumber; i++ {
			rf.nextIndex[i] = rf.LastlogIndex + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		Debug(dInfo, "S%d become a leader,it has win the %d vote", rf.me, rf.VoteCollection)
		go rf.broadcastAppendLogs()
		go rf.BrocastHeartBeat()
	}
}
func (rf *Raft) ticker() {
	for rf.killed() == false {
		Debug(dTimer, "S%d start the voteTimer", rf.me)
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 150 + (rand.Int63() % 300)
		select {
		default:
			if time.Since(rf.lastTickTime) > time.Duration(ms)*time.Millisecond {
				Debug(dTimer, "S%d voteTimer Time out", rf.me)
				rf.startElection()
			}
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	Init()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peersNumber = len(peers)
	// Your initialization code here (2A, 2B, 2C).

	rf.rfstatus = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.HeartBeatGap = 100
	rf.applyCh = applyCh
	Debug(dInfo, "S%d start to service", rf.me)
	rf.logs = make([]*Log, 1)
	rf.logs[0] = &Log{LeaderTerm: 0}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.lastTickTime = time.Now()

	rf.LastlogIndex = len(rf.logs) - 1 + rf.lastIncludedIndex
	rf.commitIndex = 0
	rf.lastApplied = 0

	// log
	rf.nextIndex = make([]int, rf.peersNumber)
	rf.matchIndex = make([]int, rf.peersNumber)
	rf.lastAppendEntriesArgs = make([]*AppendEntriesArgs, rf.peersNumber)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.appendLogToCh()
	return rf
}
