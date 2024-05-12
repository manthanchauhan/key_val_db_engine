package main

import (
	"bitcask/client/shell"
	"bitcask/client/tcp"
	"bitcask/config/constants"
	"bitcask/dataIO"
	"bitcask/dataIO/compressAndMerge"
	"bitcask/logger"
	"bitcask/test"
	"bitcask/utils"
)

func main() {
	logger.Init()

	logger.SugaredLogger.Info("Hello World")
	logger.SugaredLogger.Infof("Execution mode: %s", utils.GetExecutionMode())

	if !utils.IsExecutionModeProduction() {
		test.ClearDataLogs()
	}

	_ = dataIO.GetDataIOManager()
	go compressAndMerge.CompressionAndMergingGoRoutine()

	if !utils.IsExecutionModeProduction() {
		test.RunTests()
		test.ClearDataLogs()
	} else {
		start()
	}
}

func start() {
	switch utils.GetClientType() {

	case constants.ClientTypeShell:
		shell.GetShellClient().Run()

	case constants.ClientTypeTcp:
		tcp.GetTcpServer().Start()

	default:
		shell.GetShellClient().Run()
	}
}
