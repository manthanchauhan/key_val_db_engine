package commands

import (
	"bitcask/config/constants"
	"bitcask/dataIO"
	"bitcask/utils"
	"errors"
	"strings"
)

type writeManager struct {
}

func (w *writeManager) handler(command string) error {
	if err := w.validate(command); err != nil {
		return err
	}

	words := strings.Split(command, " ")
	key := words[1]
	value := strings.Join(words[2:], " ")

	dataIO.Write(key, value)
	return nil
}

func (w *writeManager) validate(command string) error {
	words := strings.Split(command, " ")

	if len(words) < 3 {
		return errors.New(constants.ErrMsgInvalidInput)
	}

	key := words[1]

	if err := utils.ValidateNotProtectedKeyword(key); err != nil {
		return err
	}

	value := strings.Join(words[2:], " ")

	if err := utils.ValidateNotProtectedKeyword(value); err != nil {
		return err
	}

	return nil
}
