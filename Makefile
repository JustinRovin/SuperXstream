SHELL := /bin/sh

.PHONY: all clean

all: common server

server: common
	go install xstream/server

common: deps
	go fmt xstream/netin xstream/server xstream/sg xstream/utils
	go build xstream/netin xstream/sg xstream/utils

deps:
	go get github.com/ncw/directio
	go get code.google.com/p/gcfg

clean:
	@rm -rf pkg/* bin/*
