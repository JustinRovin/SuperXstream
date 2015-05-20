SHELL := /bin/sh

.PHONY: all clean

all: common server

server: common
	go install xstream/server

common:
	go fmt xstream/net xstream/sg xstream/server
	go build xstream/net xstream/sg

clean:
	@rm -rf pkg/* bin/*
