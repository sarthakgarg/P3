package raft

import "sync"
import "github.com/cmu440/labrpc"
import "math/rand"
import "time"

//Struct for returning committed entries to the tester

type ApplyMsg struct {
	Index   int
	Command interface{}
}

//Struct for storing a log entry

type Log struct {
	Command interface{}
	Term    int
}

//Possible states in which a server could be present

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

//Struct storing all the state of a server

type Raft struct {
	mu               sync.Mutex          // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	me               int                 // this peer's index into peers[]
	state            int                 // Leader, Follower, Candidate
	currentTerm      int
	votedFor         int   // The id of the server voted for in the current term
	logs             []Log // Array of stored logs
	commitIndex      int   // Index till where the entries of the logs are committed
	nextIndex        []int
	matchIndex       []int
	applyCh          chan ApplyMsg
	numServers       int
	electionTimeout  int      // Time after which the follower becomes a candidate
	heartbeatTimeout int      // Time after which a relection is triggered
	followCh         chan int // A channel which is closed as soon as a server transitions into a follower
	votes            int      // Votes received by the server in an election
	activityCh       chan int
	consensusCh      [](chan int)
}

//GetState() returns the current term and whether the server is a leader or not

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//Structs for the requestvote rpc call

type RequestVoteArgs struct {
	Term         int
	Id           int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term int
	Vote bool
}

//Structs for the appendentries rpc calls

type AppendEntriesArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//Make the server a follower if it already is not

func (rf *Raft) makeFollower() {
	rf.votedFor = -1
	rf.state = Follower
	if rf.followCh != nil {
		select {
		case <-rf.followCh:
		default:
			close(rf.followCh)
		}
	}
}

//Function handler for the request vote rpc

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//check if the senders term is stale or not
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Vote = false
	} else {
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.makeFollower()
		}
		lastIdx := len(rf.logs) - 1
		//Check whether the senders log is atleast as uptodate as this servers log

		if args.LastLogIndex == -1 && lastIdx != -1 {
			reply.Vote = false
		} else if (rf.votedFor == -1 || rf.votedFor == args.Id) && (lastIdx < 0 || args.LastLogTerm > rf.logs[lastIdx].Term || (args.LastLogTerm == rf.logs[lastIdx].Term && args.LastLogIndex >= lastIdx)) {
			rf.votedFor = args.Id
			reply.Vote = true
			select {
			case <-rf.activityCh:
			default:
				close(rf.activityCh)
			}
		} else {
			reply.Vote = false
		}
	}
	reply.Term = rf.currentTerm
}

//return min of 2 integers

func min(i int, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

//Function handler for the appendentries rpc

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//check if the senders term is stale or not
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.currentTerm = args.Term
		rf.makeFollower()
		select {
		case <-rf.activityCh:
		default:
			close(rf.activityCh)
		}
		lastIdx := len(rf.logs) - 1
		l := len(args.Entries)
		if lastIdx < args.PrevLogIndex || l == 0 {
			reply.Success = false
		} else if args.PrevLogIndex == -1 || rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
			reply.Success = true
			rf.logs = rf.logs[:args.PrevLogIndex+1]
			for j := 0; j < len(args.Entries); j += 1 {
				rf.logs = append(rf.logs, args.Entries[j])
			}
		} else {
			reply.Success = false
		}
	}
	//Check whether the servers commitIndex can be increased based on leader commit and current term
	for j := rf.commitIndex + 1; j < min(args.LeaderCommit+1, len(rf.logs)); j += 1 {
		if rf.logs[j].Term == rf.currentTerm {
			for k := rf.commitIndex + 1; k <= j; k += 1 {
				rf.commitIndex += 1
				rf.applyCh <- ApplyMsg{rf.commitIndex + 1, rf.logs[rf.commitIndex].Command}
			}
		}
	}
}

//Function for sending an appendentries rpc call to a particular server
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.makeFollower()
		}
		rf.mu.Unlock()
	}
}

//Function for sending a requestvote rpc to a particular server
func (rf *Raft) sendRequestVote(server int, electionTimeout chan int, electionOver chan int, voteResult chan bool) {
	args := &RequestVoteArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.logs) - 1
	if args.LastLogIndex == -1 {
		args.LastLogTerm = -1
	} else {
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	}
	select {
	case <-rf.followCh:
		voteResult <- false
		rf.mu.Unlock()
		return
	default:
	}
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
		rf.makeFollower()
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

//API for starting consensus for a particular value
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	} else {
		rf.logs = append(rf.logs, Log{command, rf.currentTerm})
		for i := 0; i < rf.numServers; i += 1 {
			if i == rf.me {
				continue
			}
			select {
			case <-rf.consensusCh[i]:
			default:
				close(rf.consensusCh[i])
			}
		}
		return len(rf.logs), rf.currentTerm, true
	}
}

func (rf *Raft) Kill() {
	return
}

// function for managing timer for trigerring sending heartbeats
func (rf *Raft) SignalHeartbeat(c chan int) {
	heartbeatTimer := time.NewTimer(time.Duration(rf.heartbeatTimeout) * time.Millisecond).C
	<-heartbeatTimer
	close(c)
}

//function for managing timer for trigerring election
func (rf *Raft) SignalElection(c chan int) {
	for {
		rf.mu.Lock()
		rf.activityCh = make(chan int)
		rf.mu.Unlock()
		electionTimer := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
		select {
		case <-electionTimer:
			close(c)
			return
		case <-rf.activityCh:
		}
	}
}

//function for managing timer for trigerring reelection
func (rf *Raft) SignalReElection(c chan int) {
	electionTimer := time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond).C
	select {
	case <-electionTimer:
		close(c)
	}
}

//function for running an election
func (rf *Raft) RunElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1
	electionTimeout := make(chan int)
	electionOver := make(chan int)
	voteResult := make(chan bool)
	rf.mu.Unlock()
	for i := 0; i < rf.numServers; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, electionTimeout, electionOver, voteResult)
	}
	go rf.SignalReElection(electionTimeout)
	select {
	case k := <-voteResult:
		if k {
			close(electionOver)
			go rf.Lead()
		} else {
			close(electionOver)
			go rf.Follow()
		}
	case <-rf.followCh:
		close(electionOver)
		go rf.Follow()
	}
}

//function for sending heartbeats to all servers
func (rf *Raft) sendHeartBeat() bool {
	rf.mu.Lock()
	k := rf.currentTerm
	c := rf.commitIndex
	select {
	case <-rf.followCh:
		rf.mu.Unlock()
		return false
	default:
	}
	rf.mu.Unlock()
	for i := 0; i < rf.numServers; i++ {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{k, -1, -1, make([]Log, 0), c}
		reply := &AppendEntriesReply{0, false}
		go rf.sendAppendEntries(i, args, reply)
	}
	return true
}

//function for updating the commitIndex of the leader
func (rf *Raft) doUpdate() {
	for i := rf.commitIndex + 1; i < len(rf.logs); i += 1 {
		votes := 1
		for j := 0; j < rf.numServers; j += 1 {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				votes += 1
			}
		}
		if 2*votes <= rf.numServers {
			break
		}
		if rf.currentTerm == rf.logs[i].Term {
			for j := rf.commitIndex + 1; j <= i; j += 1 {
				rf.commitIndex += 1
				rf.applyCh <- ApplyMsg{rf.commitIndex + 1, rf.logs[rf.commitIndex].Command}
			}
		}
	}
}

// Functions for managing consensus at the leader
func (rf *Raft) waitConsensus(i int, t int) {
	rf.mu.Lock()
	k := rf.followCh
	p := rf.consensusCh[i]
	rf.mu.Unlock()
	select {
	case <-k:
		return
	case <-p:
		go rf.formConsensus(i, t)
		return
	}
}

func (rf *Raft) formConsensus(i int, t int) {
	for {
		args := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{-1, false}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		} else if rf.nextIndex[i] >= len(rf.logs) {
			rf.consensusCh[i] = make(chan int)
			go rf.waitConsensus(i, t)
			rf.mu.Unlock()
			return
		} else {
			args.Term = rf.currentTerm
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			} else {
				args.PrevLogTerm = -1
			}
			l := len(rf.logs)
			args.Entries = make([]Log, l)
			for i := 0; i < l; i += 1 {
				args.Entries[i] = rf.logs[i]
			}
			args.Entries = args.Entries[args.PrevLogIndex+1:]
			args.LeaderCommit = rf.commitIndex
		}
		rf.mu.Unlock()
		ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
		if ok {
			rf.mu.Lock()
			if reply.Term > t {
				rf.makeFollower()
				rf.mu.Unlock()
				return
			} else if reply.Success == false {
				if !(rf.state == Leader && rf.currentTerm == t) {
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[i] -= 1
			} else {
				if !(rf.state == Leader && rf.currentTerm == t) {
					rf.mu.Unlock()
					return
				}
				rf.nextIndex[i] = args.PrevLogIndex + 1 + len(args.Entries)
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				rf.doUpdate()
			}
			rf.mu.Unlock()
		}
	}
}

//Code for being in the leader state
func (rf *Raft) Lead() {
	rf.mu.Lock()
	rf.state = Leader
	currentTerm := rf.currentTerm
	for i := 0; i < rf.numServers; i += 1 {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = -1
			rf.consensusCh[i] = make(chan int)
			go rf.waitConsensus(i, currentTerm)
		}
	}
	rf.mu.Unlock()
	k := rf.sendHeartBeat()
	if !k {
		go rf.Follow()
		return
	}
	for {
		c := make(chan int)
		go rf.SignalHeartbeat(c)
		select {
		case <-c:
			k = rf.sendHeartBeat()
			if !k {
				go rf.Follow()
				return
			}
		case <-rf.followCh:
			go rf.Follow()
			return
		}
	}
}

//Code for being in a follower
func (rf *Raft) Follow() {
	rf.mu.Lock()
	rf.state = Follower
	rf.votes = 0
	rf.mu.Unlock()
	ch := make(chan int)
	go rf.SignalElection(ch)
	<-ch
	rf.mu.Lock()
	if rf.followCh != nil {
		select {
		case <-rf.followCh:
		default:
			close(rf.followCh)
		}
	}
	rf.followCh = make(chan int)
	rf.mu.Unlock()
	go rf.RunElection()
}

//Initializing the state of the servers

func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Log, 0)
	rf.commitIndex = -1
	rf.votes = 0
	rf.activityCh = make(chan int)
	rf.heartbeatTimeout = 100
	rf.numServers = len(peers)
	rf.nextIndex = make([]int, rf.numServers)
	rf.matchIndex = make([]int, rf.numServers)
	rf.consensusCh = make([](chan int), rf.numServers)
	rf.followCh = nil
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimeout = 300 + r.Intn(200)
	for i := 0; i < rf.numServers; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.applyCh = applyCh
	go rf.Follow()
	return rf
}
