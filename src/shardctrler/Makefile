default: run

COUNT = 200
TEST_NAME = "TestBasic|TestMulti"

run: clean
	go test -run ${TEST_NAME} -race -failfast > testrun.log

test: clean
	./go-test-many.sh ${COUNT} 4 ${TEST_NAME}

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

