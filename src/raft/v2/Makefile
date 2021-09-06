#./go-test-many.sh 测试次数 并行数(默认是 CPU 个数) 哪个测试

default: run

RUN_NAME="TestInitialElection2A"

run: clean
	go test -run ${RUN_NAME} -race -failfast > testrun.log

COUNT = 200
TEST_NAME = "2A|2B|2C"

test: clean
	./go-test-many.sh ${COUNT} 8 ${TEST_NAME}

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

