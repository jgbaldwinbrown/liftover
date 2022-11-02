package main

import (
	"fmt"
	"github.com/jgbaldwinbrown/liftover/pkg"
)

//	liftOver testspots2.bed over/3L.chain conversions.bed unmapped.txt

func main() {
	f := liftover.GetFlags()
	err := liftover.LiftOverFull(f)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		panic(err)
	}
}
