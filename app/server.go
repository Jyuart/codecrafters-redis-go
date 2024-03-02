package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
)

const PingMessage = "*1\r\n$4\r\nping\r\n"
const PongResponse = "+PONG\r\n"
const OkResponse = "+OK\r\n"
const NewLine = "\r\n"
const NullBulkString = "$-1\r\n"

var Dir string
var DbFileName string

type CommandType string

const (
	PING   CommandType = "ping"
	ECHO   CommandType = "echo"
	GET    CommandType = "get"
	SET    CommandType = "set"
	CONFIG CommandType = "config"
	KEYS   CommandType = "keys"
)

type Command struct {
	commandType CommandType
	params      []string
}

var storage = map[string]string{}

func main() {
	handleCommandLineArguments()

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

func handleCommandLineArguments() {
	dirFlag := flag.String("dir", "", "The directory where RDB files are stored")
	dbFileNameFlag := flag.String("dbfilename", "", "The name of the RDB file")
	flag.Parse()
	Dir = *dirFlag
	DbFileName = *dbFileNameFlag
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
		commandResult := handleCommand(command)

		_, err = conn.Write(commandResult)

		if err != nil {
			fmt.Println("There was an error writing data", err.Error())
			os.Exit(1)
		}
	}
}

func handleCommand(command Command) []byte {
	var response string

	switch command.commandType {
	case PING:
		response = PongResponse
	case ECHO:
		response = generateResponse([]string{command.params[0]}, false)
	case SET:
		key := command.params[0]
		if len(command.params) > 2 {
			expireInMs, _ := strconv.Atoi(command.params[2])
			time.AfterFunc(time.Duration(expireInMs)*time.Millisecond, func() { delete(storage, key) })
		}
		storage[key] = command.params[1]
		response = OkResponse
	case GET:
		response = getKeyValue(command)
	case CONFIG:
		flagType := command.params[0]
		flagValue := ""
		if flagType == "dir" {
			flagValue = Dir
		}
		if flagType == "dbfilename" {
			flagValue = DbFileName
		}
		response = generateResponse([]string{flagType, flagValue}, true)
	case KEYS:
		response = getRdbKeys()
	}

	return []byte(response)
}

func getRdbKeys() string {
	var keys []string

	dbFilePath := fmt.Sprint(Dir, "/", DbFileName)
	keys = rdb.GetKeys(dbFilePath)

	return generateResponse(keys, true)
}

func getKeyValue(command Command) string {
	key := command.params[0]
	if DbFileName == "" {
		return getKeyFromMemory(key)
	}
	dbFilePath := fmt.Sprint(Dir, "/", DbFileName)
	keyValue, expired := rdb.GetKeyValue(dbFilePath, key)
	if expired {
		return NullBulkString
	}
	return generateResponse([]string{keyValue}, false)
}

func getKeyFromMemory(key string) string {
	entry, ok := storage[key]
	if ok {
		return generateResponse([]string{entry}, false)
	} else {
		return NullBulkString
	}
}

func parseCommand(unparsedCommand string) Command {
	lines := strings.Split(unparsedCommand, NewLine)

	// Seems like a specific CodeCrafters case
	if len(lines) < 5 {
		return Command{commandType: PING}
	}

	switch strings.ToLower(lines[2]) {
	case "echo":
		return Command{commandType: ECHO, params: []string{lines[4]}}
	case "set":
		if len(lines) > 8 {
			return Command{commandType: SET, params: []string{lines[4], lines[6], lines[10]}}
		}
		return Command{commandType: SET, params: []string{lines[4], lines[6]}}
	case "get":
		return Command{commandType: GET, params: []string{lines[4]}}
	case "config":
		return Command{commandType: CONFIG, params: []string{lines[6]}}
	case "keys":
		return Command{commandType: KEYS, params: []string{lines[4]}}
	default:
		return Command{}
	}
}

func generateResponse(elements []string, respArrayFormat bool) string {
	response := ""
	if respArrayFormat {
		response += fmt.Sprint("*", len(elements), NewLine)
	}
	for _, el := range elements {
		response += fmt.Sprint("$", len(el), NewLine, el, NewLine)
	}

	return response
}
