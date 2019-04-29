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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

const (
	HEARTBEAT_TIMEOUT  = int64(200 * time.Millisecond)
	HEARTBEAT_INTERVAL = 150 * time.Millisecond
	UNVOTED            = -1
	NOTERM             = -1
)

type State byte

const (
	LeaderState State = iota
	CandidateState
	FollowerState
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // Latest term server has seen
	votedFor    int // CandidateId that received vote in current term
	leaderId    int
	log         []LogEntry // log entries; each entry contains command for state machine and term when entry was received by leader

	commitIndex int // index of highest log entry known to be commited
	lastApplied int // index of highest log entry applied to state machine

	heartbeatChan chan bool
	isLeaderChan  chan bool
	state         State
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LeaderState
	rf.mu.Unlock()

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

func (rf *Raft) genTimeOut() time.Duration {
	timeout := rand.Int63n(HEARTBEAT_TIMEOUT) + HEARTBEAT_TIMEOUT
	return time.Duration(timeout)
}

func (rf *Raft) resetTimeOut() {
	rf.heartbeatChan <- true
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) asFollower() {
	select {
	case <-time.After(rf.genTimeOut()):
		rf.convert2Candidate()
	case <-rf.heartbeatChan:
	}
}

func (rf *Raft) asCandidate() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.sendRequestVoteAll()
	select {
	case <-rf.isLeaderChan:
		rf.convert2Leader()
	case <-rf.heartbeatChan:
		rf.convert2Follower(NOTERM)
	case <-time.After(rf.genTimeOut()):
	}
}

func (rf *Raft) asLeader() {
	rf.sendAppendEntriesAll(nil)
	time.Sleep(HEARTBEAT_INTERVAL)
}

func (rf *Raft) sendAppendEntriesAll(command interface{}) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	peers := rf.peers
	commitIndex := rf.commitIndex
	log := rf.log
	rf.mu.Unlock()
	var lastLogTerm int
	lastLogIndex := len(log) - 1
	if lastLogIndex >= 0 {
		lastLogTerm = log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
	}
	args := &AppendEntriesArgs{
		Term:         currentTerm,
		LeaderId:     me,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
		LeaderCommit: commitIndex,
	}
	if command != nil {
		args.Entries = append(args.Entries, LogEntry{command, currentTerm})
	}
	for i := range peers {
		if i != me {
			go func(server int) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(server, args, &reply)
				rf.mu.Lock()
				toFollower := reply.Term > rf.currentTerm
				rf.mu.Unlock()
				if toFollower {
					rf.convert2Follower(reply.Term)
				}
			}(i)
		}
	}
}

func (rf *Raft) sendRequestVoteAll() {
	var lastLogTerm int
	rf.mu.Lock()
	me := rf.me
	peers := rf.peers
	currentTerm := rf.currentTerm
	log := rf.log
	rf.mu.Unlock()
	lastLogIndex := len(log) - 1
	if lastLogIndex >= 0 {
		lastLogTerm = log[lastLogIndex].Term
	} else {
		lastLogTerm = 0
	}
	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  me,
		LastLogIndex: len(log) - 1,
		LastLogTerm:  lastLogTerm,
	}
	nVoteGranted := 1
	halfPeers := len(peers) >> 1
	for i := range peers {
		if i == me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			rf.sendRequestVote(server, args, &reply)
			if reply.VoteGranted {
				rf.mu.Lock()
				nVoteGranted++
				majorityGranted := (nVoteGranted == halfPeers+1)
				rf.mu.Unlock()
				if majorityGranted {
					DPrintf("%v received majority of votes.\n", me)
					rf.isLeaderChan <- true
				}
			} else {
				rf.mu.Lock()
				toFollower := reply.Term > rf.currentTerm
				rf.mu.Unlock()
				if toFollower {
					rf.convert2Follower(reply.Term)
				}
			}
		}(i)
	}
}

func (rf *Raft) convert2Follower(term int) {
	DPrintf("%v convert to follower.\n", rf.me)
	rf.mu.Lock()
	rf.state = FollowerState
	rf.votedFor = UNVOTED
	if term != NOTERM {
		rf.currentTerm = term
	}
	rf.mu.Unlock()
}

func (rf *Raft) convert2Candidate() {
	DPrintf("%v convert to candidate.\n", rf.me)
	rf.mu.Lock()
	rf.state = CandidateState
	rf.mu.Unlock()
}

func (rf *Raft) convert2Leader() {
	DPrintf("%v convert to leader.\n", rf.me)
	rf.mu.Lock()
	rf.state = LeaderState
	rf.leaderId = rf.me
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	votedFor := rf.votedFor
	log := rf.log
	rf.mu.Unlock()
	reply.Term = currentTerm
	reply.VoteGranted = false
	if args.Term < currentTerm {
		return
	}
	if args.Term > currentTerm {
		rf.convert2Follower(args.Term)
	} else {
		rf.resetTimeOut()
	}
	if votedFor == UNVOTED || votedFor == args.CandidateId {
		lastLogIndex := len(log) - 1
		var lastLogTerm int
		if lastLogIndex >= 0 {
			lastLogTerm = log[lastLogIndex].Term
		} else {
			lastLogTerm = 0
		}
		if args.LastLogIndex >= lastLogIndex && args.LastLogTerm >= lastLogTerm {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
		}
	}
}

//
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
// within a timer interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timers around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	reply.Term = currentTerm
	if args.Term < currentTerm {
		reply.Success = false
		return
	}
	rf.mu.Lock()
	rf.leaderId = args.LeaderId
	rf.mu.Unlock()
	rf.convert2Follower(args.Term)
	rf.resetTimeOut()
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex ||
		(args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.Term) {
		reply.Success = false
		return
	}
	reply.Success = true
	if lastLogIndex > args.PrevLogIndex {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	rf.log = append(rf.log, args.Entries...)
	lastLogIndex = len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		// commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit <= lastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.sendAppendEntriesAll(command)
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == LeaderState
	rf.log = append(rf.log, LogEntry{command, term})
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatChan = make(chan bool)
	rf.isLeaderChan = make(chan bool)
	rf.leaderId = -1
	rf.state = FollowerState
	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch state {
			case FollowerState:
				rf.asFollower()
			case CandidateState:
				rf.asCandidate()
			case LeaderState:
				rf.asLeader()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
