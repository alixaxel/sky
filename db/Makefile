COVERPROFILE=/tmp/c.out

test: fmt
	go test -v

cover: fmt
	go test -coverprofile=$(COVERPROFILE)
	go tool cover -html=$(COVERPROFILE)
	rm $(COVERPROFILE)

fmt:
	go fmt

