package tcp

import (
	"bitcask/config/constants"
	"net"
	"os"
)

func StartServer() {
	port := os.Getenv(constants.TcpPort)

	if port == "" {
		panic("Set PORT in env")
	}

	l, err := net.Listen("tcp4", ":"+port)
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
	println("Listening to requests ...")

	for {
		c, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(c)
	}
}
