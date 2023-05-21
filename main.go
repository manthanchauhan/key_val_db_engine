package main

import (
	"bitcask/compressAndMerge"
	"bitcask/config/constants"
	"bitcask/disk"
	"bitcask/hashIndex"
	"bitcask/shell"
	"bitcask/tcp"
	"bitcask/test"
	"bitcask/utils"
)

func main() {
	initialize()

	if !utils.IsExecutionModeProduction() {
		test.RunTests()
	} else {
		start()
	}
}

func initialize() {
	disk.FindLatestSegmentFileName()
	hashIndex.Build()
	go compressAndMerge.CompressionAndMergingGoRoutine()
}

func start() {
	switch utils.GetClientType() {

	case constants.ClientTypeShell:
		shell.Start()

	case constants.ClientTypeTcp:
		tcp.StartServer()

	default:
		shell.Start()
	}
}
