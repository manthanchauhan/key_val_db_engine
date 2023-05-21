package main

import (
	"bitcask/compressAndMerge"
	"bitcask/disk"
	"bitcask/hashIndex"
	"bitcask/shell"
	"bitcask/test"
	"bitcask/utils"
)

func initialize() {
	disk.FindLatestSegmentFileName()
	hashIndex.Build()
	go compressAndMerge.CompressionAndMergingGoRoutine()
}

func main() {
	initialize()

	if !utils.IsExecutionModeProduction() {
		test.RunTests()
	} else {
		shell.Start()
	}
}
