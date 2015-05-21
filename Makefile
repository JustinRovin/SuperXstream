SHELL := /bin/sh

.PHONY: all clean

all: common server

server: common
	go install xstream/server

client: common
	go install xstream/client

common: deps
	go fmt xstream/netin xstream/sg xstream/server
	go build xstream/netin xstream/sg

deps:
	go get github.com/ncw/directio

clean:
	@rm -rf pkg/* bin/*
