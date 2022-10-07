#!/bin/bash
set -e

./install.sh

liftconcat \
	-l ISO1 \
	-i testconc.bed \
	-c 3L.chain

rm unmapped.txt

liftconcat \
	-l W501 \
	-i testdata5.pairs \
	-c all.chain \
	-t "1,2" \
	-o lift_part1.pairs

liftconcat \
	-l W501 \
	-i lift_part1.pairs \
	-c all.chain \
	-t "3,4" \
	-o lift_part2.pairs

liftconcat \
	-l W501 \
	-i testdata5.pairs.gz \
	-c all.chain \
	-t "1,2" \
	-o lift_part1.pairs.gz

liftconcat \
	-l W501 \
	-i lift_part1.pairs.gz \
	-c all.chain \
	-t "3,4" \
	-o lift_part2.pairs.gz
