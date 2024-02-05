package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

const PingMessage = "*1\r\n$4\r\nping\r\n"
const PongResponse = "+PONG\r\n"

type CommandType string

const (
	ECHO CommandType = "echo"
	GET CommandType = "get"
	SET CommandType = "set"
)

type Command struct {
	commandType CommandType
	params []string
}

var storage = map[string]string{}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go listenAndRespond(conn)
	}
}

func listenAndRespond(conn net.Conn) {
	for {
		buff := make([]byte, 128)
		readBytes, err := conn.Read(buff)
		if err != nil {
			continue
		}

		readString := string(buff)[0:readBytes]

		command, err := parseEchoCommand(readString)

		if err == nil {
			var response string

			switch command.commandType {
			case ECHO:
				response = fmt.Sprint("$", len(command.params[0]), "\r\n", command.params[0], "\r\n")
			case SET:
				key := command.params[0]
				storage[key] = command.params[1]
				response = "*1\r\n$2\r\nOK\r\n"
			case GET:
				key := command.params[0]
				value := storage[key]
				response = fmt.Sprint("*2\r\n", "$", len(value), "\r\n", value, "\r\n")
			}

			_, err = conn.Write([]byte(response))
		} else {
			_, err = conn.Write([]byte(PongResponse))
		}

		if err != nil {
			fmt.Println("There was an error writing data", err.Error())
			os.Exit(1)
		}
	}
}

func parseEchoCommand(unparsedCommand string) (Command, error) {
	lines := strings.Split(unparsedCommand, "\r\n")

	if len(lines) < 3 {
		return Command{}, errors.New("Not a full command")
	}

	switch lines[2] {
	case "echo":
		return Command{commandType: ECHO, params: []string{ lines[4] }}, nil
	case "set":
		return Command{commandType: SET, params: []string{ lines[4], lines[6] }}, nil
	case "get":
		return Command{commandType: GET, params: []string{ lines[4] }}, nil
	default:
		return Command{}, errors.ErrUnsupported
	}
}
