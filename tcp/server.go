package tcp

import (
	"fmt"
	"net"
	"os"
)

func startServer() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide a port number!")
		return
	}

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		panic(err)
	}

	defer func(l net.Listener) {
		err := l.Close()
		if err != nil {
			panic(err)
		}
	}(l)

	listenToConnRequests(l)
}

func listenToConnRequests(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			panic(err) // todo
		}
		go handleConnection(c)
	}
}
