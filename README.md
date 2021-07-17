## 6.824 Spring 2021 Labs

### [Lab 1: MapReduce](http://nil.csail.mit.edu/6.824/2021/labs/lab-mr.html)

A few rules:

- mr.MakeCoordinator 规定了 reduce tasks 个数
- reduce task output file 命名方式：mr-out-X，X为第X个reduce task
- mr-out-X的format形式 必须与 mrsequential.go中的一致
- map task 生成的中间结果 也放到 当前目录下，提供给 reduce task
- coordinator需要当所有 reduce tasks结束后，exit
- coordinator exit后，worker自己也要exit

Hints:

- worker发送RPC向coordinator要task，task包含了待处理的文件名
- map reduce的功能函数，通过Go Plugin方式在runtime load进来，参考Makefile
- mr里文件改动后，需要rebuild wc.so
- 这个lab里的worker sharing 同一个 file system，但Paper里Worker是通过GFS sharing
- map task 产生的中间文件 命名方式：mr-X-Y，X为map task number，Y为reduce task number，从reduce角度来看，会读取所有mr-*-R的文件
- 中间文件 存储方式可以用json
- map task partition key函数使用 ihash(key) % R(the number of reduce tasks)
- 可以参考mrsequential.go
- coordinator 需要考虑加锁 处理共享数据
- 使用Go race detector: --race
- reduce task worker需要等待所有map task worker完成才能开始，需要考虑如何处理worker等待
- coordinator无法真实确认worker是crash了，或hang住了，还是处理比较慢。所以最好的办法是，wait一段时间后重新assign新的reduce task
- 如果要实现Backup Tasks，也应该是在一个长时间之后，比如lab中的10s
- crash / recovery 的测试，可以使用mrapps/crash.go
- 为了保证写文件时的原子性，即all-or-nothing，paper中trick的一点：先写入tempFile，最终写完后再rename
- test-mr.sh会将所有文件 写入mr-tmp中
- test-mr-many.sh提供参数可以重复跑test-mr.sh，注意test-mr.sh无法并行跑，因为会共用一个socket file

Challenges:

- 实现自己的MapReduce application，如 Distributed Grep
- 让coordinator和worker在真实的不同机器上运行，read/write files使用shared file system 比如S3

### Lab 1 总结

#### Atomicity 的保证

- reassign的task，可能会被重复执行，即worker A被认为unhealthy后，仍然生成了file，接替A的worker B也会生成相同的file。
  此时不论是谁最终写完数据，由于代码逻辑一样，因此数据文件也都是一样的。都可以通过os.rename来保证最终只存在一个文件
- 中间文件 和 结果文件 名称的讲究：同一个task生成的文件名必须一致，为了保证多次执行task，只会有一个文件的原子性

#### Logging策略

- 在main logic主函数里，不打err的日志，只记录 成功或retry 日志
- err的日志，在main logic调用的function里记录
- main logic调用function A，A以下的function日志尽量避免，留给function A记录

#### test-mr.sh在macOS上注意点

- 没有timeout，需要brew install gtimeout
- 提示wait: -n: invalid option，需要brew upgrade bash，之前默认的版本是version 3.2.57(1)-release

