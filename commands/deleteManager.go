package commands

import (
	"bitcask/config/constants"
	"bitcask/dataIO"
	"bitcask/utils"
	"errors"
	"strings"
)

type deleteManager struct {
}

func (d *deleteManager) handler(command string) error {
	if err := d.validate(command); err != nil {
		return err
	}

	key := strings.Split(command, " ")[1]
	dataIO.Delete(key)

	return nil
}

func (d *deleteManager) validate(command string) error {
	words := strings.Split(command, " ")

	if len(words) != 2 {
		return errors.New(constants.ErrMsgInvalidInput)
	}

	key := words[1]

	if err := utils.ValidateNotProtectedKeyword(key); err != nil {
		return err
	}

	return nil
}
