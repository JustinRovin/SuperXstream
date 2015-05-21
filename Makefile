SHELL := /bin/sh

.PHONY: all clean

all: common server

server: common
	go install xstream/server

common:
	go fmt xstream/netin xstream/sg xstream/server
	go build xstream/netin xstream/sg

clean:
	@rm -rf pkg/* bin/*
