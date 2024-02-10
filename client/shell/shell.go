package shell

import (
	"bitcask/commands"
	"bitcask/config/constants"
	"bitcask/utils"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func Start() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Simple Shell")
	fmt.Println("---------------------")

	var command string

	for true {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		command = strings.Replace(text, "\n", "", -1)

		words := strings.Split(command, " ")

		if len(words) == 0 {
			fmt.Println("Invalid input")
			continue
		}

		operation := strings.ToUpper(words[0])

		if operation == constants.CommandExit {
			break
		}

		switch operation {
		case constants.CommandWrite:
			writeCommandShell(command)
			break
		case constants.CommandRead:
			val := readCommandShell(command)
			fmt.Println(val)
			break
		case constants.CommandDelete:
			deleteCommandShell(command)
			break
		default:
			fmt.Println("Invalid input")
		}
	}
}

func readCommandShell(command string) string {
	if utils.IsExecutionModeProduction() {
		defer getDefer()()
	}

	op, err := commands.ReadCommand(command)
	if err != nil {
		panic(err)
	}

	return op
}

func writeCommandShell(command string) {
	if utils.IsExecutionModeProduction() {
		defer getDefer()()
	}

	err := commands.WriteCommand(command)
	if err != nil {
		panic(err)
	}
}

func deleteCommandShell(command string) {
	if utils.IsExecutionModeProduction() {
		defer getDefer()()
	}

	err := commands.DeleteCommand(command)
	if err != nil {
		panic(err)
	}
}

func getDefer() func() {
	return func() {
		if r := recover(); r != nil {
			_, ok := r.(string)

			if ok {
				fmt.Println(r)
			} else {
				fmt.Println(r.(error).Error())
			}
		}
	}
}
