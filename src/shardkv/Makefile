default: test

COUNT = 200
#TEST_NAME = "TestStaticShards|TestJoinLeave|TestSnapshot|TestMissChange|TestConcurrent1"
#TEST_NAME = "TestConcurrent2|TestConcurrent3|TestUnreliable1|TestUnreliable2|TestUnreliable3"
TEST_NAME = "TestConcurrent2"
#TEST_NAME = "TestChallenge1Delete|TestChallenge2Unaffected|TestChallenge2Partial"

test: clean
	./go-test-many.sh ${COUNT} 1 ${TEST_NAME}

clean:
	@rm *.log || true
	@rm tester || true
	@rm log || true

