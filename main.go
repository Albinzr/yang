package main

import (
	"runtime"

	reader "applytics.in/yang/src"
)

func main() {
	runtime.GOMAXPROCS(2)
	reader.Start()
}
