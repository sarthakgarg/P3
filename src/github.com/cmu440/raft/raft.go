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

//import "fmt"

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
	state            int
	currentTerm      int
	votedFor         int
	logs             []Log
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	applyCh          chan ApplyMsg
	numServers       int
	electionTimeout  int
	heartbeatTimeout int
	followCh         chan int
	votes            int
	activityCh       chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	//	fmt.Println(rf.me, term, isleader)
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
	Success bool
}

//
// example RequestVote RPC handler.
//
//TODO:Implement the interrupt, leading to state change

//TODO: Heavy concurrency between RPC calls and all other methods, protect access to raft state using a global mutex

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//TODO: Modify for handling logs! Before voting check if the candidate is up to date
	//TODO: Where should the reply.votedFor reset to zero? => start of a new term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Vote = false
	} else {
		if rf.currentTerm < args.Term {

			rf.currentTerm = args.Term

			rf.votedFor = -1
			//Signal to become a follower if not
			if rf.followCh != nil {
				select {
				case <-rf.followCh:
				default:
					close(rf.followCh)
				}
			}
		}
		if rf.votedFor == -1 || rf.votedFor == args.Id {
			rf.votedFor = args.Id
			reply.Vote = true
			//TODO: think about moving this to condition rf.votedFor == -1
			select {
			case <-rf.activityCh:
			default:
				close(rf.activityCh)
			}
		}
	}
	reply.Term = rf.currentTerm
}

//TODO: Implement the interrupts in append and request vote rpc
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//Toy implementation for checkpoint, just compare the term and return
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		//Can be a heartbeat from some other newly appointed leader
		if rf.currentTerm == args.Term {
			reply.Success = true
		} else {

			rf.currentTerm = args.Term

			rf.votedFor = -1
		}
		if rf.followCh != nil {
			select {
			case <-rf.followCh:
			default:
				close(rf.followCh)
			}
		}
		select {
		case <-rf.activityCh:
		default:
			close(rf.activityCh)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, completeCh chan int) {
	rf.peers[server].Call("Raft.AppendEntries", args, reply)
	close(completeCh)
}

/*
This function repeatedly sends requestVotes to a server until it succeed. However, election timeout occurs, electionOver channel must be closed! If vote is given, votes are incremented. If the server wins the election, then voteResult is signalled True, if server does not get the vote, then nothing is signalled. If the term received is greater than current term, then currentTerm is incremented and a false is passed over voteResult to signal to the candidate to convert to a follower!
*/
func (rf *Raft) sendRequestVote(server int, electionTimeout chan int, electionOver chan int, voteResult chan bool) {
	args := &RequestVoteArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	rf.mu.Unlock()
	args.Id = rf.me
	reply := &RequestVoteReply{}
	reply.Term = 0
	reply.Vote = false
	for {
		var ok bool
		select {
		case <-electionTimeout:
			select {
			case voteResult <- false:
			case <-electionOver:
			}
			return
		default:
			ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		}
		if ok {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {

		rf.currentTerm = reply.Term
		rf.votedFor = -1

		select {
		case voteResult <- false:
		case <-electionOver:
		}
	} else if reply.Vote {
		rf.votes += 1
		if 2*rf.votes <= rf.numServers {
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

//Code for signalling for sending a heartbeat

func (rf *Raft) SignalHeartbeat(c chan int) {
	heartbeatTimer := time.NewTimer(time.Duration(rf.heartbeatTimeout) * time.Millisecond).C
	<-heartbeatTimer
	close(c)
}

//Code for signalling for an election.
//rf.validRPC must be invoked in conjunction with a default upon receiving heartbeat appendEntries rpc from the current leader or granting a vote to a candidate

func (rf *Raft) SignalElection(c chan int) {
	//TODO: Think of ways to manage validRPC
	for {
		rf.activityCh = make(chan int)
		electionTimer := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
		select {
		case <-electionTimer:
			close(c)
			return
		case <-rf.activityCh:
		}
	}
}

func (rf *Raft) SignalReElection(c chan int) {
	electionTimer := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
	select {
	case <-electionTimer:
		close(c)
	}
}

//Code for being in the candidate state (running election)
//Right now the state of the graph is accessed unprotected
func (rf *Raft) RunElection() {
	//	fmt.Printf("%d is now candidate\n", rf.me)
	rf.mu.Lock()
	rf.state = Candidate
	//Increment current term and vote for self
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1
	//Start timer and wait for 3 outcomes: timeout, valid heartbeat or winning election.
	electionTimeout := make(chan int)
	electionOver := make(chan int)
	voteResult := make(chan bool)
	rf.mu.Unlock()
	for i := 0; i < rf.numServers; i++ {
		if i == rf.me {
			continue
		}
		//TODO: think about whether we should keep retrying failed requestVote rpcs
		go rf.sendRequestVote(i, electionTimeout, electionOver, voteResult)
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
	case <-rf.followCh:
		//Somebody else probably won the election
		close(electionOver)
		go rf.Follow()
	}
}

//Code for sending a heartbeat to all servers
func (rf *Raft) sendHeartBeat() bool {
	for i := 0; i < rf.numServers; i++ {
		select {
		case <-rf.followCh:
			return false
		default:
		}
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		k := rf.currentTerm
		rf.mu.Unlock()
		args := &AppendEntriesArgs{k}
		reply := &AppendEntriesReply{0, false}
		//		fmt.Printf("%d is now Stuck\n", rf.me)
		completeCh := make(chan int)
		go rf.sendAppendEntries(i, args, reply, completeCh)
		select {
		case <-rf.followCh:
			return false
		case <-completeCh:
		}
		//		fmt.Printf("%d is now UnStuck\n", rf.me)

		if reply.Term > k {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			rf.currentTerm = reply.Term
			rf.votedFor = -1

			return false
		}
	}
	return true
}

//Code for being a leader
//For now the leader just sends a heartbeat periodically to all servers
func (rf *Raft) Lead() {
	//	fmt.Printf("%d is now Leader\n", rf.me)
	rf.mu.Lock()
	rf.state = Leader
	rf.mu.Unlock()
	k := rf.sendHeartBeat()
	if !k {
		go rf.Follow()
		return
	}
	//TODO: Fix, right now, just sending heartbeats at regular intervals
	for {
		c := make(chan int)
		go rf.SignalHeartbeat(c)
		//	fmt.Printf("%d is now waiting!\n", rf.me)
		select {
		case <-c:
		case <-rf.followCh:
			go rf.Follow()
			return
		}
		k = rf.sendHeartBeat()
		if !k {
			go rf.Follow()
			return
		}
	}
}

//Code for being in the follower state
func (rf *Raft) Follow() {
	//TODO: I guess the state of rf need not be protected.
	//	fmt.Printf("%d is now follower\n", rf.me)
	rf.mu.Lock()
	rf.state = Follower
	rf.votes = 0
	rf.mu.Unlock()
	ch := make(chan int)
	go rf.SignalElection(ch)
	//Wait for the election timeout signal, till then remain in the follower state and serve RPCs
	<-ch
	rf.followCh = make(chan int)
	go rf.RunElection()
}

func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {

	//Initialize the state to be kept in the raft
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Log, 1)
	rf.commitIndex = 0
	rf.votes = 0
	rf.activityCh = make(chan int)
	rf.heartbeatTimeout = 100
	rf.lastApplied = 0
	rf.numServers = len(peers)
	rf.nextIndex = make([]int, rf.numServers)
	rf.matchIndex = make([]int, rf.numServers)
	//followCh is used to signal by the the RPC calls to transition into the follower state from the leader or the candidate state. It is made once the server transitions out from the follower state
	rf.followCh = nil
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	//Election timeout is randomly chosen between 300 to 500 milliseconds, however it is kept fixed for the lifetime of the server right now
	rf.electionTimeout = 300 + r.Intn(200)

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
