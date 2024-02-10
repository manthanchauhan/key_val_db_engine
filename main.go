package main

import (
	"bitcask/client/shell"
	"bitcask/client/tcp"
	"bitcask/config/constants"
	"bitcask/dataIO"
	"bitcask/logger"
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
		shell.GetShellClient().Run()

	case constants.ClientTypeTcp:
		tcp.GetTcpServer().Start()

	default:
		shell.GetShellClient().Run()
	}
}
