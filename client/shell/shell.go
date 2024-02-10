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

type Client struct {
	Reader *bufio.Reader
}

func (c *Client) Run() {
	fmt.Println("DB Shell")
	fmt.Println("---------------------")

	var command string

	for true {
		fmt.Print("-> ")
		text, _ := c.Reader.ReadString('\n')

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
			c.writeHandler(command)
			break
		case constants.CommandRead:
			c.readHandler(command)
			break
		case constants.CommandDelete:
			c.deleteHandler(command)
			break
		default:
			fmt.Println("Invalid input")
		}
	}
}

func (c *Client) readHandler(command string) {
	if utils.IsExecutionModeProduction() {
		defer c.recoverFromAllErrors()()
	}

	value, err := commands.ReadCommand(command)

	if err != nil {
		panic(err)
	}

	fmt.Println(value)
}

func (c *Client) writeHandler(command string) {
	if utils.IsExecutionModeProduction() {
		defer c.recoverFromAllErrors()()
	}

	err := commands.WriteCommand(command)
	if err != nil {
		panic(err)
	}
}

func (c *Client) deleteHandler(command string) {
	if utils.IsExecutionModeProduction() {
		defer c.recoverFromAllErrors()()
	}

	err := commands.DeleteCommand(command)
	if err != nil {
		panic(err)
	}
}

func (c *Client) recoverFromAllErrors() func() {
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

func GetDefaultShellClient() *Client {
	client := Client{Reader: bufio.NewReader(os.Stdin)}
	return &client
}
