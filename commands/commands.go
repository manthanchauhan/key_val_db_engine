package commands

import (
	"bitcask/config/constants"
	"bitcask/dataIO"
	"errors"
	"strings"
)

func Exec(command string) (string, error) {
	words := strings.Split(command, " ")

	if len(words) == 0 {
		return "", errors.New("invalid input")
	}

	operation := strings.ToUpper(words[0])

	if operation == constants.CommandExit {
		return "", nil
	}

	switch operation {
	case constants.CommandWrite:
		return "", WriteCommand(command)
	case constants.CommandRead:
		return ReadCommand(command)
	default:
		return "", errors.New("invalid input")
	}
}

func ReadCommand(command string) (op string, err error) {
	defer getDefer(&err)()

	words := strings.Split(command, " ")

	if len(words) < 2 {
		return "", errors.New("invalid input")
	}

	key := words[1]
	return dataIO.Read(key), nil
}

func WriteCommand(command string) (err error) {
	defer getDefer(&err)()

	words := strings.Split(command, " ")

	if len(words) < 3 {
		return errors.New("invalid input")
	}

	key := words[1]
	value := strings.Join(words[2:], " ")

	dataIO.Write(key, value)
	return nil
}

func getDefer(err *error) func() {
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
