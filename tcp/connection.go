package tcp

import (
	"bitcask/commands"
	"bitcask/config/constants"
	"bufio"
	"fmt"
	"net"
	"strings"
)

func handleConnection(c net.Conn) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())

	for {
		input, err := readInput(c)
		if err != nil {
			panic(err)
		}

		if input == constants.CommandExit {
			break
		}

		result, err := commands.Exec(input)

		if result == "" && err != nil {
			result = err.Error()
		}

		writeOutput(result, c)
	}

	err := c.Close()
	if err != nil {
		panic(err)
	}
}

func readInput(c net.Conn) (string, error) {
	netData, err := bufio.NewReader(c).ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSpace(netData), nil
}

func writeOutput(output string, c net.Conn) {
	_, err := c.Write([]byte(output))

	if err != nil {
		panic(err)
	}
}
