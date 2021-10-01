## 6.824 Spring 2021 Labs

### [Lab 3: Fault-tolerant Key/Value Service](http://nil.csail.mit.edu/6.824/2021/labs/lab-kvraft.html)


This lab implemented all parts (Clerk, Service, and Raft) shown in the [diagram of Raft interactions](http://nil.csail.mit.edu/6.824/2021/notes/raft_diagram.pdf).

需要实现3个operations：

- Put(key, value)：replaces现有的key与value
- Append(key, arg)：appends arg to key's value
- Get(key)：fetch key不存在返回empty string

Each client talks to the service through a Clerk with Put/Append/Get methods. 

A Clerk manages RPC interactions with the servers.

这个service必须提供strong consistency 给Clerk的Get/Put/Append方法

完善kvraft代码

#### Part A

每一个kvservers都包含一个raft peer。Clerks发送Put() Append() Get() RPCs给 kvservers。

kvserver提交operations到Raft，raft log会记录这些operations，进而按顺序apply operation。

Task：

- client.go中实现Put/Append/Get
- each server需要执行Op command通过raft commit

Hit：

- raft.Start()后，kvservers等待raft完成arguments。因此需要一直等待applyCh
- 可以不用优化read-only operation在Section 8中


