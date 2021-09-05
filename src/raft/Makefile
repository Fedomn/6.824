#./go-test-many.sh 测试次数 并行数(默认是 CPU 个数) 哪个测试

default: test

COUNT = 200
TEST_NAME = "2A|2B|2C"

test: clean
	./go-test-many.sh ${COUNT} 8 ${TEST_NAME}

#testrace: clean
#	for ((i=1; i <= ${COUNT}; ++i)) ; do \
#		go test -run ${TEST_NAME} -race -failfast -count ${COUNT} > testrace.log ; \
#	done

#testrace: clean
#	go test -run ${TEST_NAME} -race -failfast -count 100 > testrace.log

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

#2B:
#	time go test -run TestBasicAgree2B -race > 2B.log
