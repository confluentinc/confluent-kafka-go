# Convenience helper Makefile for simplifying tasks in all sub-dirs
# of this git repo.
#
# Usage (from top-level dir):
#   make -f mk/Makefile "<go-subcommand>"
#
# E.g., to run 'go vet' on all Go code:
#   make -f mk/Makefile vet
# 
#
#

DIRS?=$(shell find . -xdev -type f -name '*.go' -exec dirname {} \; | sort | uniq)

%:
	@(for d in $(DIRS) ; do (cd "$$d" && go $@) ; done)
