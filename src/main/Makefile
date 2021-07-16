clean:
	rm mr-* || true

# lab 1
mrapps: clean
	go build -race -buildmode=plugin ../mrapps/wc.go
	go build -race -buildmode=plugin ../mrapps/crash.go

seqrun: mrapps
	go run -race mrsequential.go wc.so pg*.txt

coord:
	go run -race mrcoordinator.go pg-*.txt

worker:
	go run -race mrworker.go wc.so
