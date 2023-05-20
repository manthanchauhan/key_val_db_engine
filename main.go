package main

import (
	"bitcask/disk"
	"bitcask/hashIndex"
	"bitcask/shell"
	"bitcask/test"
	"bitcask/utils"
)

func initialize() {
	disk.SetLatestSegmentFileName()
	hashIndex.Build()
	//go compressAndMerge.CompressionAndMergingGoRoutine()
}

func main() {
	initialize()

	if !utils.IsExecutionModeProduction() {
		test.RunTests()
	} else {
		shell.Start()
	}
}
