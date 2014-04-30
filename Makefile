PACKAGES=core db query server skyd/config
PKGPATHS=$(patsubst %,github.com/skydb/sky/%,$(PACKAGES))
GO=/usr/local/go/bin/go

REPO=github.com/skydb/sky
CWD=$(shell pwd)
ifndef GOPATH
GOPATH=`pwd`/.go
endif
GOBIN=$(GOPATH)/bin

all: env fmt build test

env:
	@echo "CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS)"
	@echo "GOPATH=$(GOPATH) GOBIN=$(GOBIN)"
	mkdir -p $(GOBIN)
	mkdir -p $(GOPATH)/src/`dirname $(REPO)`
	[ -d $(GOPATH)/src/$(REPO) ] || ln -sfv $(CWD) $(GOPATH)/src/$(REPO)

get: env
	$(GO) get github.com/stretchr/testify
	$(GO) get -d ./...

build: get
	$(GO) build -a -o bin/skyd ./skyd/main.go

test:
	${MAKE} -C db test
	${MAKE} -C query test
	${MAKE} -C server test

fmt:
	$(GO) fmt $(PKGPATHS)

