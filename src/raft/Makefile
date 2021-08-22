#./go-test-many.sh 测试次数 并行数(默认是 CPU 个数) 哪个测试

default: test

#LOOPS = 100
#2A:
#	for ((i=1; i <= ${LOOPS}; ++i)) ; do \
#		go test -run 2A -race > log ; \
#	done

COUNT = 10
TEST_NAME = TestFailNoAgree2B

test: clean
	./go-test-many.sh ${COUNT} 8 ${TEST_NAME}

2A-count:
	go test -run TestReElection2A -race -failfast -count 1 > log

clean:
	@rm test-*.log || true
	@rm tester || true
	@rm log || true

2B:
	time go test -run TestBasicAgree2B -race > 2B.log
