default: run

COUNT = 200
TEST_NAME = "3A|3B"

run: clean
	go test -run ${TEST_NAME} -race -failfast > testrun.log

test: clean
	./go-test-many.sh ${COUNT} 1 ${TEST_NAME}

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

