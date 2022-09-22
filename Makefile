.PHONY: test
test: 
	@go test -race -covermode=atomic -v -coverprofile=coverage.txt ./... || exit 1;
	@for dir in `find . -type f -name "go.mod" -exec dirname {} \;`; do \
		if [ $$dir != "." ]; then \
			cd $$dir; \
			go test -race -covermode=atomic -v -coverprofile=coverage.txt ./... || exit 1; \
			cd - > /dev/null ;\
			lines=`cat $$dir/coverage.txt | wc -l`; \
			lines=`expr $$lines - 1`; \
			tail -n $$lines $$dir/coverage.txt >> coverage.txt; \
		fi; \
	done

.PHONY: benchmark
benchmark: 
	@for dir in `find . -type f -name "go.mod" -exec dirname {} \;`; do \
		cd $$dir; \
		go test -bench=. -run=^Benchmark ./...; \
		cd - > /dev/null; \
	done