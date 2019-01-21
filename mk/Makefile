# Convenience helper Makefile for simplifying tasks in all sub-dirs
# of this git repo.
#
# Usage (from top-level dir):
#   make -f mk/Makefile "<command> <args..>"
#
# E.g., to run 'go vet' on all Go code:
#   make -f mk/Makefile "go vet"
# 
#
#

DIRS?=$(shell find . -xdev -type f -name '*.go' -exec dirname {} \; | sort | uniq)

docs: generr
	mk/doc-gen.py > kafka/api.html

generr:
	cd kafka/go_rdkafka_generr && go install
	go generate ./...

%:
	@(for d in $(DIRS) ; do (cd "$$d" && $@) ; done)
