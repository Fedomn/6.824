default: test

COUNT = 200
TEST_NAME = "Test"

test: clean
	./go-test-many.sh ${COUNT} 1 ${TEST_NAME}

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

