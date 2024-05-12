package dataIO

import (
	"bitcask/config/constants"
	"bitcask/dataIO/index"
	"bitcask/logger"
	"bitcask/utils"
)

var singletonDataIOManager *Manager

func GetDataIOManager() *Manager {
	if singletonDataIOManager != nil {
		return singletonDataIOManager
	}

	singletonDataIOManager = &Manager{index: index.GetConfiguredIndex()}
	return singletonDataIOManager
}

type Manager struct {
	index index.Index
}

func (m *Manager) ReadHandler(key string) string {
	logger.SugaredLogger.Info("Reading key - ", key)

	val, err := m.index.Get(key)

	if err != nil {
		panic(err)
	}

	logger.SugaredLogger.Info("Found val - ", val)

	if utils.EqualsIgnoreCase(val, constants.DeletedValuePlaceholder) {
		panic(constants.ErrMsgNotFound)
	}

	return val
}

func (m *Manager) WriteHandler(key string, val string) {
	logger.SugaredLogger.Infof("Writing key - %s, val - %s", key, val)

	if err := m.index.Set(key, val); err != nil {
		panic(err)
	}

	logger.SugaredLogger.Info("Written")
}

func (m *Manager) DeleteHandler(key string) {
	logger.SugaredLogger.Infof("Deleting key - %s", key)

	if err := m.index.Delete(key); err != nil {
		panic(err)
	}

	logger.SugaredLogger.Info("Deleted")
}

func (m *Manager) CompressAndMerge() {
	m.index.Compress()
	m.index.Merge()
}
