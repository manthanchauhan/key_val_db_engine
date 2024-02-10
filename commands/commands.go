package commands

import (
	"bitcask/config/constants"
	"bitcask/dataIO"
	"errors"
	"fmt"
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

	if len(words) != 2 {
		return "", errors.New("invalid input")
	}

	key := words[1]

	if checkIfKeyword(key) {
		return "", errors.New(fmt.Sprintf("cannot use '%s' since it is a protected keyword", key))
	}

	return dataIO.Read(key), nil
}

func WriteCommand(command string) (err error) {
	defer getDefer(&err)()

	words := strings.Split(command, " ")

	if len(words) != 3 {
		return errors.New("invalid input")
	}

	key := words[1]
	value := strings.Join(words[2:], " ")

	if checkIfKeyword(value) {
		return errors.New(fmt.Sprintf("cannot use '%s' since it is a protected keyword", value))
	}

	if checkIfKeyword(key) {
		return errors.New(fmt.Sprintf("cannot use '%s' since it is a protected keyword", key))
	}

	dataIO.Write(key, value)
	return nil
}

func DeleteCommand(command string) (err error) {
	defer getDefer(&err)()

	words := strings.Split(command, " ")

	if len(words) != 2 {
		return errors.New("invalid input")
	}

	key := words[1]

	if checkIfKeyword(key) {
		return errors.New(fmt.Sprintf("cannot use '%s' since it is a protected keyword", key))
	}

	dataIO.Delete(key)
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

func checkIfKeyword(val string) bool {
	val = strings.TrimSpace(val)

	for _, keyword := range constants.Keywords {
		if strings.ToUpper(keyword) == strings.ToUpper(val) {
			return true
		}
	}

	return false
}
