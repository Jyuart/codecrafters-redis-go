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
		buff := make([]byte, 1024)
		readBytes, err := conn.Read(buff)
		if err != nil {
			return
		}

		readString := string(buff)[0:readBytes]

		echoCommandParam, err := parseEchoCommand(readString)
		if err == nil {
			response := fmt.Sprint("$", len(echoCommandParam), "\r\n", echoCommandParam, "\r\n")
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

func parseEchoCommand(command string) (string, error) {
	lines := strings.Split(command, "\r\n")

	if lines[2] != "echo" {
		return "", errors.New("Not an ECHO command")
	}

	return lines[4], nil
}
