## 6.824 Spring 2021 Labs

### [Lab 2: Raft](http://nil.csail.mit.edu/6.824/2021/labs/lab-raft.html)

#### overview

raft实现需要支持以下接口

1. `rf := Make(peers, me, persister, applyCh)`

Make(peers,me,...) 是来创建一个raft server，其中peers参数是：集群中的所有servers的network标识，me是当前server所在peers的index

2. `rf.Start(command interface{}) (index, term, isleader)`

Start(command) 是ask raft开始处理append command到replicated logs，这个函数会立即return，不会等待log append完成

3. `rf.GetState() (term, isLeader)`

返回currentTerm 和 当前server是否认为自己是leader

4. `type ApplyMsg`

--- 

每当一个new log entry在raft中被committed后，each raft peer都应该send ApplyMsg给service，为了tester使用

raft.go中包含`sendRequestVote()`来处理`RequestVote RPC`。raft peers使用`labrpc`进行 RPC通信。
labrcp中包含了delay，re-order，discard去模拟network的各种问题。

#### Part 2A leader election hints

Task:

实现Raft leader election 和 heartbeats。Part 2A goal是single leader to be elected。
如果leader no failure，则它会一致保持leader的状态，否则新的leader会当选。

Hints:

- 不能直接run Raft实现，而是使用test方式run。`go test -run 2A -race`
- follow Raft Paper的**Figure 2**。这Part关心的是send和receive RequestVote RPC
- 需要添加**Figure 2**中的state给leader election服务，实现在Raft struct中。需要定义一个struct来存储log entry
- 完成RequestVoteArgs和RequestVoteReply两个struct。
  修改`Make()`方法去创建一个后台协成 去periodically发送RequestVote 开始leader选举(当server election timeout内没有收到其它peer请求)。  
  实现`RequestVote()` RPC handle
- 为了实现heartbeat，需要定义`AppendEntries` RPC struct。然后leader会periodically发送heartbeat给followers。
  实现AppendEntries Handler去reset它的election timeout。
- 确保不同的peers的election timeout不会在同一时间fire。否则所有peers都不会当选。简单说来就是满足: `broadcastTime << election timeout`
- 测试要求 leader发送heartbeat RPC次数要求：**每1秒 不超过 10次**
- 测试要求 老leader发生failure的5s之内，需要有新的leader当选。
  因此需要保证election timeout足够短，让election能够在5s之内完成多轮，防止出现split vote情况。
- Raft Paper Section5.2中提到election timeout在150~300ms之间。
  但只有当leader发送heartbeat的频率远高于 每150ms一次时，这样的范围才有意义。即 **heartbeat时间 远小于150ms**
  但是，测试要求 每1s不超过10次 心跳(100ms心跳间隔)，因此倒推出election timeout大于paper里的150ms到300ms，但也不能太大，否则5s只能不能选出leader
- 使用Go的rand
- 使用time.Sleep()去等待一段时间，不要使用time.Timer或time.Ticker它们比较困难使用
- [guidance page](http://nil.csail.mit.edu/6.824/2021/labs/guidance.html)包含dev和debug
- 如果test fail，需要重读Paper Figure 2部分，因为leader election包含在多个parts of figure里
- 不要忘记实现`GetState()`
- 测试 会call Raft的 `rf.Kill()`方法，当测试将要永远shutdown一个Raft instance时。
  需要在所有loop里判断kill是否调佣，为了防止dead raft instance还在print confusing message
- Go RPC只会发送struct里首字母大写的fields。labgob会提示warning

Result:

如果passed测试会显示: `... Passed --   4.0  3   32    9170    0`

其中的数字含义为:

- `4.0`: the time that the test took in seconds
- `3`: the number of Raft peers (usually 3 or 5)
- `32`: the number of RPCs sent during the test
- `9170`: the total number of bytes in the RPC messages
- `0`: the number of log entries that Raft reports were committed

#### Part 2A notes

##### labrpc

代码强依赖于labrpc.go，它是一个channel-based RPC，来发送gob-encoded values，本质通过方法反射调用 模拟RPC

Network: 一个集合 network，clients，servers。它可以AddServer。
它的核心在`MakeNetwork`，会有创建一个goroutine去处理`rn.endCh`来的pseudo RPC请求，从这里更好的理解channel-based RPC

ClientEnd: 客户端的end-point，用来talk to server 如方法
`end.Call("Raft.AppendEntries", &args, &reply)` 发送一个RPC，等待reply

Service: 一个对象，它包含一些方法，可以用来 RPC call

Server: collection of services，它们共享相同的 rpc dispatcher

ClientEnd calls: 可以并发请求，但到达server的order并不保证

MakeService(receiverObject): 和Go的rpcs.Register()相似，注册一个object

##### raft/config

提供给Raft tester使用，所以Config struct里包含了：测试的Raft实例，logs，network等等一系列状态，用来后续assert

endnames: 一个二维数组，每个将要发送到的 端口文件名称。
二维数组的第一个idx是：raft server idx。第二个idx是：raft server对应的peers idx

##### candidate处理 send or receive RPC逻辑在一个goroutine里

RPC的req和rsp处理逻辑 要放在一个goroutine里，不要使用channel等待收集所有的RPC rsp后再处理，
因为RPC call可能会**delay很久**才有rsp。

同时election ticker需要时刻保持timeout循环，为了保证即使RPC请求**delay很久**了，
但在election timeout后，仍然会从开始一个新的term+1的election。

这里term+1，是因为candidate是并发给所有servers发送的RPC，存在着已经grant term的server，
所以新一轮的RequestVote RPC必须将term+1，才能满足follower only vote for first ask candidate。

##### revert to follower的情况

all servers rules中包含一条：If RPC request or response contains term T > currentTerm:
set currentTerm = T, convert to follower.

这句话的意义：只要server遇到了higher term的RPC，自己就会revert到follower。

leader election情况中：每次必须有higher term来抢占到majority的server，才能当选leader；
如果被其它相同term的 candidate抢先一些server，无法到达majority，则会进入下一轮election

##### RequestVote与AppendEntries 代码思想

需要通过并发的视角 来看待每一行代码，比如：
一个goroutine在运行400行代码的逻辑，但另外一个goroutine运行到了500行逻辑， 从而改变了内部状态 影响了400行的逻辑。

因此，在这种并发程序中，状态修改之前，仍需要double check，防止前一时刻别的goroutine已经做了相同change，

#### Part 2B log hints

Tasks: 实现append new log entries

Hints:

- 第一步通过测试`TestBasicAgree2B`。通过实现`Start()`和`AppendEntries`逻辑来send/receive new log entries
- 需要实现election restriction。Paper section 5.4.1
- loop check方法需要有个pause。使用Cond或time.Sleep在loop iteration

#### Part 2C persistence

This requires that Raft keep persistent state that survives a reboot

Task:

- 完成raft.go里的persist()和readPersist()方法
- 在需要的地方调用persist()方法

#### Part 2D log compaction

snapshot作用：

- 让raft减少log占用空间
- 落后的raft，可以通过snapshot快速追上leader

Task：

实现Snapshot、CondInstallSnapshot、InstallSnapshot RPC

Hits：

- send the entire snapshot in a single InstallSnapshot RPC
- old log entries必须没有reachable references，才能被GC掉
- raft logs不能在通过数组index来决定log entry index，需要重新实现
- 即使log trimmed了，AppendEntries仍然需要term和index，需要考虑snapshot里包含lastIncludedTerm/lastIncludedIndex
- raft必须store每个snapshot在persister object中SaveStateAndSnapshot方法