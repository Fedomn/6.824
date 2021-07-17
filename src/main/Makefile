clean:
	rm -rf mr-* || true

# lab 1
mrapps: clean
	cd ../mrapps && go build --race -buildmode=plugin ./wc.go
	cd ../mrapps && go build --race -buildmode=plugin ./indexer.go
	cd ../mrapps && go build --race -buildmode=plugin ./mtiming.go
	cd ../mrapps && go build --race -buildmode=plugin ./rtiming.go
	cd ../mrapps && go build --race -buildmode=plugin ./jobcount.go
	cd ../mrapps && go build --race -buildmode=plugin ./early_exit.go
	cd ../mrapps && go build --race -buildmode=plugin ./crash.go
	cd ../mrapps && go build --race -buildmode=plugin ./nocrash.go

seqrun:
	go run -race mrsequential.go wc.so pg*.txt

coord:
	go run -race mrcoordinator.go ./pg*.txt

worker:
	go run -race mrworker.go ../mrapps/early_exit.so
