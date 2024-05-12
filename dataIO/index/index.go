package index

import (
	"bitcask/config/constants"
	"bitcask/dataIO/index/hashIndex"
	"bitcask/dataIO/index/lsmIndex"
	"bitcask/logger"
	"bitcask/utils"
	"fmt"
)

func GetConfiguredIndex() Index {
	var index Index

	switch utils.GetIndexType() {
	case constants.IndexTypeHashIndex:
		index = hashIndex.GetHashIndex()
	case constants.IndexTypeLSMIndex:
		index = lsmIndex.GetLsmIndex()

	default:
		panic(fmt.Sprintf("Invalid index type - %s", utils.GetIndexType()))
	}

	logger.SugaredLogger.Info("Using Index Type - ", utils.GetIndexType())

	return index
}
