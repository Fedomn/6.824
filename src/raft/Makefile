#./go-test-many.sh 测试次数 并行数(默认是 CPU 个数) 哪个测试

default: test

COUNT = 200
TEST_NAME = "2A|2B|2C|2D"

run: clean
	go test -run ${TEST_NAME} -race -failfast > testrun.log

test: clean
	./go-test-many.sh ${COUNT} 4 ${TEST_NAME}

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

