package commands

import (
	"bitcask/config/constants"
	"bitcask/dataIO"
	"bitcask/utils"
	"errors"
	"strings"
)

type readManager struct {
	dataIOManager *dataIO.Manager
}

func (m *readManager) handler(command string) (value string, err error) {
	if err := m.validate(command); err != nil {
		return "", err
	}

	key := strings.Split(command, " ")[1]

	return m.dataIOManager.ReadHandler(key), nil
}

func (m *readManager) validate(command string) error {
	words := strings.Split(command, " ")

	if len(words) != 2 {
		return errors.New(constants.ErrMsgInvalidInput)
	}

	key := words[1]

	if err := utils.ValidateNotProtectedKeyword(key); err != nil {
		return err
	}

	if key == "" {
		return errors.New(constants.ErrMsgInvalidInput)
	}

	return nil
}
