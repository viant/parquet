package main

import (
	"github.com/viant/parquet/parquet/cmd"
	"os"
)

var Version string = "1.0"

func main() {
	args := os.Args
	cmd.RunClient(Version, args)
}