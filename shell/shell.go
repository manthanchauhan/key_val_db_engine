package shell

import (
	"bitcask/commands"
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

		if operation == "EXIT" {
			break
		}

		switch operation {
		case "WRITE":
			commands.WriteCommand(command)
			break
		case "READ":
			val := commands.ReadCommand(command)
			fmt.Println(val)
			break
		default:
			fmt.Println("Invalid input")
		}
	}
}
