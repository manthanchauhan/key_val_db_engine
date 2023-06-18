package main

import (
	"bitcask/config/constants"
	"bitcask/dataIO"
	"bitcask/logger"
	"bitcask/shell"
	"bitcask/tcp"
	"bitcask/test"
	"bitcask/utils"
)

func main() {
	logger.Init()

	logger.SugaredLogger.Info("Hello World")

	dataIO.Init()

	if !utils.IsExecutionModeProduction() {
		test.RunTests()
	} else {
		start()
	}
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
