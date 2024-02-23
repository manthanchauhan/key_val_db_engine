package commands

import (
	"bitcask/config/constants"
	"bitcask/dataIO"
	"errors"
	"strings"
)

var singletonManager *Manager

func GetCommandManager() *Manager {
	if singletonManager != nil {
		return singletonManager
	}

	singletonManager = &Manager{
		readManager:   &readManager{dataIOManager: dataIO.GetDataIOManager()},
		writeManager:  &writeManager{dataIOManager: dataIO.GetDataIOManager()},
		deleteManager: &deleteManager{dataIOManager: dataIO.GetDataIOManager()},
	}

	return singletonManager
}

type Manager struct {
	readManager   *readManager
	writeManager  *writeManager
	deleteManager *deleteManager
}

func (m *Manager) ReadHandler(command string) (value string, err error) {
	defer m.recoverPanicIntoErrorObj(&err)()
	return m.readManager.handler(command)
}

func (m *Manager) WriteHandler(command string) (err error) {
	defer m.recoverPanicIntoErrorObj(&err)()
	return m.writeManager.handler(command)
}

func (m *Manager) DeleteHandler(command string) (err error) {
	defer m.recoverPanicIntoErrorObj(&err)()
	return m.deleteManager.handler(command)
}

func (m *Manager) handler(command string) (string, error) {
	words := strings.Split(command, " ")

	if len(words) == 0 {
		return "", errors.New(constants.ErrMsgInvalidInput)
	}

	operation := strings.ToUpper(words[0])

	if operation == constants.CommandExit {
		return "", nil
	}

	switch operation {
	case constants.CommandWrite:
		return "", m.WriteHandler(command)
	case constants.CommandRead:
		return m.ReadHandler(command)
	case constants.CommandDelete:
		return "", m.DeleteHandler(command)
	default:
		return "", errors.New("invalid input")
	}
}

func (m *Manager) recoverPanicIntoErrorObj(err *error) func() {
	return func() {
		if r := recover(); r != nil {
			var ok bool

			if *err, ok = r.(error); !ok {
				*err = errors.New(r.(string))
			}

			return
		}
	}
}
