#!/bin/bash
set -e

go build liftconcat.go

cp liftconcat ~/mybin
cp lift.sh ~/mybin/lift
chmod +x ~/mybin/lift
