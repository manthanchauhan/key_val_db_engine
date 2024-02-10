package tcp

import (
	"bitcask/commands"
	"bitcask/config/constants"
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

type connectionHandler struct {
	connection     net.Conn
	CommandManager *commands.Manager
}

func (c *connectionHandler) handle() {
	fmt.Printf("Serving %s\n", c.connection.RemoteAddr().String())

	c.write("Connected")

	for {
		input, err := c.read()

		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if input == constants.CommandExit {
			break
		}

		result, err := c.executeCommand(input)

		if result == "" && err != nil {
			result = err.Error()
		}

		c.write(result)
	}

	err := c.connection.Close()

	if err != nil {
		panic(err)
	}

	fmt.Printf("Disconnected from %s\n", c.connection.RemoteAddr().String())

}

func (c *connectionHandler) executeCommand(command string) (string, error) {
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
		return "", c.CommandManager.WriteHandler(command)
	case constants.CommandRead:
		return c.CommandManager.ReadHandler(command)
	case constants.CommandDelete:
		return "", c.CommandManager.DeleteHandler(command)
	default:
		return "", errors.New(constants.ErrMsgInvalidInput)
	}
}

func (c *connectionHandler) read() (string, error) {
	netData, err := bufio.NewReader(c.connection).ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSpace(netData), nil
}

func (c *connectionHandler) write(output string) {
	if output != "" {
		output += "\n"
	}

	_, err := c.connection.Write([]byte(output))

	if err != nil {
		panic(err)
	}
}
