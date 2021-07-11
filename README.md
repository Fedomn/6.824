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
