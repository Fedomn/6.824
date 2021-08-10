#./go-test-many.sh 测试次数 并行数(默认是 CPU 个数) 哪个测试

default: 2B

#LOOPS = 100
#2A:
#	for ((i=1; i <= ${LOOPS}; ++i)) ; do \
#		go test -run 2A -race > log ; \
#	done

COUNT = 200
TEST_NAME = 2A

test: clean
	./go-test-many.sh ${COUNT} 8 ${TEST_NAME}

2A-count:
	go test -run TestManyElections2A -race -failfast -count 50 > log

clean:
	@rm test-*.log || true
	@rm tester || true

2B:
	time go test -run TestBasicAgree2B -race > 2B.log
