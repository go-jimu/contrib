MODULES := $(shell find . -name go.mod -not -path './.git/*' -exec dirname {} \; | sort)

# test runs every module's tests in parallel, printing a module's output only
# when it fails, then merges the coverage profiles into coverage.txt.
.PHONY: test
test:
	@printf '%s\n' $(MODULES) | xargs -P 4 -I{} sh -c '\
		out=$$(cd {} && go test -race -covermode=atomic -coverprofile=coverage.part ./... 2>&1) \
		|| { echo "=== FAIL {}"; echo "$$out"; exit 1; }; echo "ok {}"'
	@echo "mode: atomic" > coverage.txt
	@for dir in $(MODULES); do tail -n +2 $$dir/coverage.part >> coverage.txt; rm -f $$dir/coverage.part; done

# tidy-check fails when a module's go.mod or go.sum is not tidy on its own.
# go.work hides a missing requirement during development and in make test,
# but users build each module without the workspace.
.PHONY: tidy-check
tidy-check:
	@for dir in $(MODULES); do \
		(cd $$dir && GOWORK=off go mod tidy -diff) || { echo "=== $$dir: run GOWORK=off go mod tidy"; exit 1; }; \
	done

.PHONY: benchmark
benchmark:
	@for dir in $(MODULES); do \
		(cd $$dir && go test -bench=. -run=^Benchmark ./...) || exit 1; \
	done
