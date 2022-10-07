#!/bin/bash
set -e

(cd cmd && (
	ls *.go | while read i ; do
		go build $i
	done
))

cp cmd/liftconcat ~/mybin
cp lift.sh ~/mybin/lift
chmod +x ~/mybin/lift
