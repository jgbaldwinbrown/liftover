#!/bin/bash
set -e

./install.sh

liftconcat \
	-l ISO1 \
	-i testconc.bed \
	-c 3L.chain

rm unmapped.txt
