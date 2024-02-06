package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const PingMessage = "*1\r\n$4\r\nping\r\n"
const PongResponse = "+PONG\r\n"
const OkResponse = "+OK\r\n"
const NewLine = "\r\n"
const NullBulkString = "$-1\r\n"

type CommandType string

const (
	PING CommandType = "ping"
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

		command := parseCommand(readString)

		var response string

		switch command.commandType {
		case PING:
			response = PongResponse
		case ECHO:
			response = generateRespone(command.params[0])
		case SET:
			key := command.params[0]
			if len(command.params) > 2 {
				expireInMs, _ := strconv.Atoi(command.params[2]) 
				time.AfterFunc(time.Duration(expireInMs) * time.Millisecond,  func() { delete(storage, key) })
			}
			storage[key] = command.params[1]
			response = OkResponse
		case GET:
			key := command.params[0]
			entry, ok := storage[key]
			if ok {
				response = generateRespone(entry)
			} else {
				response = NullBulkString
			}
		}

		_, err = conn.Write([]byte(response))

		if err != nil {
			fmt.Println("There was an error writing data", err.Error())
			os.Exit(1)
		}
	}
}

func parseCommand(unparsedCommand string) Command {
	lines := strings.Split(unparsedCommand, "\r\n")

	if len(lines) < 5 {
		return Command{commandType: PING}
	}

	switch lines[2] {
	case "echo":
		return Command{commandType: ECHO, params: []string{ lines[4] }}
	case "set":
		if len(lines) > 8 {
			return Command{commandType: SET, params: []string{ lines[4], lines[6], lines[10] }}
		}
		return Command{commandType: SET, params: []string{ lines[4], lines[6] }}
	case "get":
		return Command{commandType: GET, params: []string{ lines[4] }}
	default:
		return Command{}
	}
}

func generateRespone(response string) string {
	return fmt.Sprint("$", len(response), NewLine, response, NewLine)
}
