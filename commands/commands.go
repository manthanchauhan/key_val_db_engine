package commands

import (
	"bitcask/dataIO"
	"bitcask/utils"
	"fmt"
	"strings"
)

func ReadCommand(command string) string {
	if utils.IsExecutionModeProduction() {
		defer getDefer()()
	}

	words := strings.Split(command, " ")

	if len(words) < 2 {
		panic("Invalid input")
	}

	key := words[1]
	return dataIO.Read(key)
}

func WriteCommand(command string) {
	if utils.IsExecutionModeProduction() {
		defer getDefer()()
	}

	words := strings.Split(command, " ")

	if len(words) < 3 {
		panic("Invalid input")
	}

	key := words[1]
	value := strings.Join(words[2:], " ")

	dataIO.Write(key, value)
}

func getDefer() func() {
	return func() {
		if r := recover(); r != nil {
			_, ok := r.(string)

			if ok {
				fmt.Println(r)
			} else {
				fmt.Println(r.(error))
			}
		}
	}
}
