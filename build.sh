#!/bin/bash

# Config
export GOPATH=$HOME/go:$HOME

# Build script

cd
tar xzmf ~/slug.tar
touch Makefile

make
