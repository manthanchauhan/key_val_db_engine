package tcp

import (
	"bitcask/commands"
	"net"
	"os"
)

var singletonServer *Server

func GetTcpServer() *Server {
	if singletonServer != nil {
		return singletonServer
	}

	singletonServer = &Server{
		Port:               os.Getenv("PORT"),
		Network:            "tcp4",
		connectionListener: nil,
	}

	return singletonServer
}

type Server struct {
	Port               string
	Network            string
	connectionListener net.Listener
}

func (s *Server) Start() {
	s.createConnectionListener()
	defer s.closeConnectionListener()()
	s.startConnectionListening()
}

func (s *Server) createConnectionListener() {
	var err error

	if s.connectionListener, err = net.Listen(s.Network, ":"+s.Port); err != nil {
		panic(err)
	}
}

func (s *Server) startConnectionListening() {
	println("Listening to requests ...")

	for {
		c, err := s.connectionListener.Accept()

		if err != nil {
			panic(err)
		}

		handler := connectionHandler{
			connection:     c,
			CommandManager: commands.GetCommandManager(),
		}

		go handler.handle()
	}
}

func (s *Server) closeConnectionListener() func() {
	return func() {
		err := s.connectionListener.Close()
		if err != nil {
			panic(err)
		}
	}
}
