clean:
	rm mr-* || true

# lab 1
wc:
	go build -race -buildmode=plugin ../mrapps/wc.go

crash:
	go build -race -buildmode=plugin ../mrapps/crash.go

seqrun: wc
	go run -race mrsequential.go wc.so pg*.txt

coord:
	go run -race mrcoordinator.go pg-*.txt

worker: wc clean
	go run -race mrworker.go wc.so
