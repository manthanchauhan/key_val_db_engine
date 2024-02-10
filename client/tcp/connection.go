package tcp

import (
	"bitcask/commands"
	"bitcask/config/constants"
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
)

func handleConnection(c net.Conn) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())

	writeOutput("Connected", c)

	for {
		input, err := readInput(c)
		if err == io.EOF {
			break
		} else if err != nil {
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

	fmt.Printf("Disconnected from %s\n", c.RemoteAddr().String())

}

func readInput(c net.Conn) (string, error) {
	netData, err := bufio.NewReader(c).ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSpace(netData), nil
}

func writeOutput(output string, c net.Conn) {
	if output != "" {
		output += "\n"
	}

	_, err := c.Write([]byte(output))

	if err != nil {
		panic(err)
	}
}
