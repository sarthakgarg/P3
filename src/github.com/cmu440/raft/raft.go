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

import "sync"
import "github.com/cmu440/labrpc"
import "math/rand"
import "time"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Log struct {
	Command interface{}
	Term    int
}

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           int
	currentTerm     int
	votedFor        int
	logs            []Logs
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	applyCh         chan ApplyMsg
	numServers      int
	electionTimeout int
	validRPC        chan int
	validHeartbeat  chan int
	votes           int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	Id   int
	//TODO:Log checking to be implemented after the checkpoint
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	Vote bool
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term    int
	Success int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//TODO: Modify for handling logs! Before voting check if the candidate is up to date
	//TODO: Where should the reply.votedFor reset to zero? => start of a new term
	if rf.currentTerm > args.Term {
		reply.Vote = false
	} else {
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		if rf.votedFor == nil || rf.votedFor == args.Id {
			rf.votedFor = args.Id
			reply.Vote = true
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

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
/*
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
*/

func (rf *Raft) sendAppendEntries(server int) {
	//Define Args and Reply
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

/*
This function repeatedly sends requestVotes to a server until it succeed. However, election timeout occurs, electionOver channel must be closed! If vote is given, votes are incremented. If the server wins the election, then voteResult is signalled True, if server does not get the vote, then nothing is signalled. If the term received is greater than current term, then currentTerm is incremented and a false is passed over voteResult to signal to the candidate to convert to a follower!
*/
func (rf *Raft) sendRequestVote(server int, electionTimeout chan int, electionOver chan int, voteResult chan bool) {
	args = &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.Id = rf.me
	reply = &RequestVoteReply{}
	reply.Term = 0
	reply.Vote = false
	for {
		select {
		case <-electionTimeout:
			select {
			case voteResult <- false:
			case <-electionOver:
			}
			return
		default:
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if ok {
				break
			}
		}
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		select {
		case voteResult <- false:
		case <-electionOver:
		}
	} else if reply.Vote {
		//TODO: acquire mutex around the following block
		rf.votes += 1
		if 2*rf.votes < rf.numServers {
			return
		}

		select {
		case voteResult <- true:
		case <-electionOver:
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	// Your code here (3B).

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
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

//Code for signalling for an election.
//rf.validRPC must be invoked in conjunction with a default upon receiving heartbeat appendEntries rpc from the current leader or granting a vote to a candidate

func (rf *Raft) SignalElection(c chan int) {
	//TODO: Think of ways to manage validRPC
	electionTimer := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
	for {
		select {
		case <-electionTimer:
			close(c)
			break
		case <-rf.validRPC:
			electionTimer := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
		}
	}
}

func (rf *Raft) SignalReElection(c chan int) {
	electionTimer := time.newTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
	select {
	case <-electionTimer:
		close(c)
	}
}

//Code for being in the candidate state (running election)
//Right now the state of the graph is accessed unprotected
func (rf *Raft) RunElection() {
	rf.state = Candidate
	//Increment current term and vote for self
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1
	//Start timer and wait for 3 outcomes: timeout, valid heartbeat or winning election.
	electionTimeout := make(chan int)
	electionOver := make(chan int)
	voteResult := make(chan bool)
	for i := 0; i < rf.numServers; i++ {
		if i == rf.me {
			continue
		}
		rf.sendRequestVote(i, electionTimeout, electionOver, voteResult)
	}
	go rf.SignalReElection(electionTimeout)
	select {
	case k := <-voteResult:
		if k {
			//Elected leader
			close(electionOver)
			go rf.Lead()
		} else {
			//Go to follower state
			close(electionOver)
			go rf.Follow()
		}
		//TODO: insert case when heartbeat from a competitor is received. Use validHeartbeat channel
	}
}

//Code for being a leader
func (rf *Raft) Lead() {

}

//Code for being in the follower state
func (rf *Raft) Follow() {
	rf.state = Follower
	ch := make(chan int)
	go rf.SignalElection(ch)
	//Wait for the election timeout signal, till then remain in the follower state and serve RPCs
	<-ch
	go rf.RunElection()
}

func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {

	//Initialize the state to be kept in the raft
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Log, 1)
	rf.commitIndex = 0
	rf.votes = 0
	rf.lastApplied = 0
	rf.numServers = len(peers)
	rf.nextIndex = make([]int, rf.numServers)
	rf.matchIndex = make([]int, rf.numServers)
	rf.validRPC = make(chan int)
	rf.validHeartbeat = nil
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	//Election timeout is randomly chosen between 1000 to 2000 milliseconds, however it is kept fixed for the lifetime of the server right now
	rf.electionTimeout = 1000 + r.Intn(1000)

	for i := 0; i < rf.numServers; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.applyCh = applyCh

	//Register for RPC
	//I believe that methods in config.go does that

	//Launch the server in the follower state and return
	go rf.Follow()
	return rf
}
