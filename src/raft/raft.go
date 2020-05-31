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
// rf.GetState() (term, )
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
	"github.com/sirupsen/logrus"
)

// import "bytes"
// import "../labgob"

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

type State string

const (
	Follower  State = "Follower"
	Candidate       = "Candidate"
	Leader          = "Leader"
)

type Log struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	// Persistent State
	// latest term server has seen
	currentTerm int
	// candidateID that receivedVote in currentTerm, or -1 if None
	votedFor int
	// log entries, each entry contains command for state machine, and term when entries was received by leader
	log []Log

	// Volatile state on all servers
	// Index of highest log entry known to be commited
	commitIndex int
	// Index of highest log entry applied to state
	lastApplied int

	// Volatile state on leader
	// For each server, index of next log entry to send to that server
	nextIndex []int
	// For each server, index of highest log entry known to be replicated on server
	matchIndex []int

	ElectionTimer      *time.Timer
	resetElectionTimer chan bool
	winElectChan       chan bool
	voteGrantedChan    chan bool

	votesReceived int
	logger        *logrus.Entry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool

	term = rf.currentTerm
	isLeader = rf.state == Leader
	// rf.logger.Tracef("Current state term=%d isLeader=%v", term, isLeader)
	return term, isLeader
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

type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.logger.Tracef("Received a vote request from server=%d with term=%d currentTerm=%d", args.CandidateID, args.Term, rf.currentTerm)

	rf.mu.Lock()
	rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.logger.Tracef("Vote request from %d term is stale", args.CandidateID)
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && (args.Term >= rf.currentTerm) {
		// TODO: candidate's log is at least up to date as receiver's log
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.voteGrantedChan <- true
	}
	rf.logger.Tracef("Voted for %d", rf.votedFor)
	//	1. Reply false if term < currentTerm
	//	2. If votedFor is null or candidateId, and candidate’s log is at
	//	   least as up-to-date as receiver’s log, grant vote
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.logger.Infof("Sending vote request to %d", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != Candidate || rf.currentTerm != args.Term {
			return ok
		}

		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
		}

		if reply.VoteGranted {
			rf.votesReceived++
			if rf.votesReceived > len(rf.peers)/2 {
				rf.winElectChan <- true
			}
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		rf.mu.Lock()
		if server != rf.me && rf.state == Candidate {
			reply := RequestVoteReply{}
			go rf.sendRequestVote(server, &args, &reply)
		}
		rf.mu.Unlock()
	}
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

type AppendEntriesRequest struct {
	// Leader's Term
	Term int
	// So followers can redirect Client
	LeaderID int
	// Index of log entry immediately preceding new ones
	PrevLogIndex int
	// Term of prevLogIndex Entry
	PrevLogTerm int
	// Log entries to store (empty for heartbeat; may send multiple)
	Entries []Log

	// Leader's commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// currentTerm for leader to update itself
	Term int
	// If follower contained entry matching prevLogIndex and prevLogTerm
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	rf.logger.Tracef("Sending appendEntries to server=%d", server)
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	return ok
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.logger.Tracef("Received heartbeat from server=%d term=%d", args.LeaderID, args.Term)
	rf.resetElectionTimer <- true
	reply.Success = true
	reply.Term = rf.currentTerm
	//	1. Reply false if term < currentTerm
	//  2. Reply false if log doesn’t contain an entry at prevLogIndex
	//  whose term matches prevLogTerm
	//  3. If an existing entry conflicts with a new one (same index
	//  but different terms), delete the existing entry and all that
	//  follow it
	//  4. Append any new entries not already in the log
	//  5. If leaderCommit > commitIndex, set commitIndex =
	//     min(leaderCommit, index of last new entry)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.logger.Tracef("Server %d has been killed", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

func (rf *Raft) SendHeartbeats() {
	for i := 0; i < len(rf.peers); i += 1 {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := AppendEntriesRequest{
				Term:     rf.currentTerm,
				LeaderID: rf.me,
			}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				rf.logger.Tracef("Could not send a heartbeat to %d", server)
			}
		}(i)
	}
}

func (rf *Raft) startServer() {
	heartbeatInterval := time.Duration(150) * time.Millisecond
	minElectionTimeoutMS := 300
	maxElectionTimeoutMS := 500

	for {
		electionTimeoutMS := time.Duration(rand.Intn(maxElectionTimeoutMS-minElectionTimeoutMS+1)+minElectionTimeoutMS) * time.Millisecond
		switch rf.state {
		case Follower:
			{
				rf.logger.Infof("I am a follower")
				select {
				case <-rf.resetElectionTimer:
					{
						rf.logger.Tracef("Received reset election timer")
					}
				case <-rf.voteGrantedChan:
				case <-time.After(electionTimeoutMS):
					{
						rf.state = Candidate
					}
				}
			}
		case Candidate:
			{
				rf.logger.Infof("I am a candidate")
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.votesReceived++
				rf.mu.Unlock()

				rf.logger.Tracef("Sending vote requests out for election term=%d", rf.currentTerm)
				go rf.broadcastRequestVote()

				select {
				case <-rf.resetElectionTimer:
					{
						rf.logger.Tracef("Received reset election timer, setting to follower")
						rf.state = Follower
					}
				case <-rf.winElectChan:
					{
						rf.logger.Tracef("I am the leader now")
						rf.mu.Lock()
						rf.state = Leader
						rf.mu.Unlock()
					}
				case <-time.After(electionTimeoutMS):
				}
			}
		case Leader:
			{
				rf.logger.Infof("I am a leader")
				go rf.SendHeartbeats()
				select {
				case <-rf.resetElectionTimer:
				case <-rf.winElectChan:
				case <-time.After(heartbeatInterval):
				}
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:              peers,
		persister:          persister,
		me:                 me,
		currentTerm:        0,
		votedFor:           -1,
		commitIndex:        0,
		lastApplied:        0,
		state:              Follower,
		resetElectionTimer: make(chan bool),
		voteGrantedChan:    make(chan bool),
		winElectChan:       make(chan bool),
		logger:             logrus.WithFields(logrus.Fields{"ID": me}),
	}

	logrus.SetLevel(logrus.TraceLevel)

	go rf.startServer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
