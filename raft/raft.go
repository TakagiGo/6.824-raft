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
	TickerEvent
	VoteEvent
	StopEvent
)

type Event struct {
	eventType EventType
	eventData interface{}
}

type Log struct {
	leaderTerm int
	command    interface{}
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
	//2A
	log            []*Log
	logLength      int
	currentTerm    int
	votedFor       int
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	rfstatus       Rfstatus
	processCh      chan *Event
	applyCh        chan ApplyMsg
	tickerTimer    *time.Timer
	cancelLeader   chan bool
	VoteCollection int
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {

	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
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
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.votedFor != -1 || rf.currentTerm > args.Term || (rf.rfstatus == leader && rf.currentTerm >= args.Term) {
		Debug(dInfo, "S%d receive a Vote Request from %d,but have reject it", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		Debug(dInfo, "S%d receive a Vote Request from %d, vote for it", rf.me, args.CandidateId)
		rf.tickerTimer.Reset(time.Duration(1) * time.Second)
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		if rf.rfstatus == leader {
			rf.rfstatus = follower
			rf.cancelLeader <- true
		}
		//todo 2B
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dWarn, "S%d reset tickerTimer", rf.me)
	rf.tickerTimer.Reset(time.Duration(1) * time.Second)
	if rf.rfstatus == candidate {
		rf.rfstatus = follower
	}
	// Your code here (2A, 2B).
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
	rf.processCh <- &Event{eventType: VoteEvent, eventData: reply}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	atomic.StoreInt32(&rf.dead, 1)
	Debug(dError, "S%d is be killed", rf.me)
	rf.close()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
}

func (rf *Raft) processEvent() {
	for rf.killed() == false {
		if rf.processCh == nil {
			return
		}
		event := <-rf.processCh
		if event == nil {
			return
		}
		switch event.eventType {
		case TickerTimeOutEvent:
			Debug(dTimer, "S%d voteTimer Time out", rf.me)
			rf.votedFor = -1
			rf.startElection()
		case TickerEvent:
			Debug(dInfo, "S%d receive a Ticker Event from", rf.me)
		case VoteEvent:
			Debug(dVote, "S%d recive a vote Event", rf.me)
			rf.VoteCollection++
			if rf.VoteCollection > rf.peersNumber/2 {
				rf.mu.Lock()
				rf.rfstatus = leader
				rf.mu.Unlock()
				Debug(dInfo, "S%d become a leader,it has win the %d vote", rf.me, rf.VoteCollection)

				rf.sendticker()
			}
		default:
			return
		}
	}
}

func (rf *Raft) sendticker() {

	sendtickerTimer := time.NewTimer(time.Duration(100) * time.Millisecond)
	for rf.killed() == false {
		select {
		case <-sendtickerTimer.C:
			Debug(dTimer, "S%d send ticker to maintain the leader", rf.me)
			// Your code here (2A)
			// Check if a leader election should be started.
			rf.tickerTimer.Reset(time.Duration(1) * time.Second)
			sendtickerTimer.Reset(time.Duration(100) * time.Millisecond)
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int) {
					args := &AppendEntriesArgs{}
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, args, reply)
					if !ok {
						Debug(dError, "S%d send AppendEntries to %d failed", rf.me, i)
					}
				}(i)
			}
		case <-rf.cancelLeader:
			return

		}
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.VoteCollection = 0
	var wg sync.WaitGroup
	wg.Add(rf.peersNumber)
	rf.rfstatus = candidate
	rf.mu.Unlock()
	rf.currentTerm++
	rf.tickerTimer.Reset(time.Duration(1) * time.Second)
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		//todo (2B)
	}
	rf.processCh <- &Event{eventType: VoteEvent, eventData: &RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}}
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
			}
		}(i, args)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		Debug(dTimer, "S%d start the voteTimer", rf.me)
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.tickerTimer.C:
			rf.processCh <- &Event{eventType: TickerTimeOutEvent}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
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
	rf.applyCh = applyCh
	Debug(dInfo, "S%d start to service", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.processCh = make(chan *Event, 256)
	rf.cancelLeader = make(chan bool, 2)
	rf.tickerTimer = time.NewTimer(time.Duration(50+(rand.Int63()%300)) * time.Millisecond)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.processEvent()
	return rf
}

func (rf *Raft) close() {
DrainLoop:
	for {
		select {
		case <-rf.processCh:
		default:
			break DrainLoop
		}
	}
	close(rf.processCh)
	close(rf.applyCh)
	close(rf.cancelLeader)
	rf.tickerTimer.Stop()
}
