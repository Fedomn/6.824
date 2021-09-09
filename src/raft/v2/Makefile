#./go-test-many.sh 测试次数 并行数(默认是 CPU 个数) 哪个测试

default: test

RUN_NAME="TestFigure8Unreliable2C"

run: clean
	go test -run ${RUN_NAME} -race -failfast -count 10 > testrun.log

COUNT = 200
TEST_NAME = "2C"
#TEST_NAME = "2A|TestBasicAgree2B|TestRPCBytes2B|TestFailAgree2B|TestFailNoAgree2B|TestConcurrentStarts2B|TestFailNoAgree2B|TestConcurrentStarts2B"

test: clean
	./go-test-many.sh ${COUNT} 8 ${TEST_NAME}

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

