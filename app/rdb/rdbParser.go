package rdb

import (
	"bytes"
	"os"
)

const RESIZE_DB = 0xFB
const DB_END = 0xFF

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func GetKeyValue(rdbFilePath string, key string) string {
	fileData := parseFile(rdbFilePath)
	resizeDbCodePosition := getOpCodePosition(fileData, RESIZE_DB)
	keysLen := getKeysLen(string(fileData), resizeDbCodePosition)

	keysStartPosition := getKeysStartPosition(fileData)
	for i := 0; i < keysLen; i++ {
		currentKeyLen := int(fileData[keysStartPosition])
		currentKeyEndPosition := keysStartPosition + currentKeyLen + 1
		currentKey := fileData[keysStartPosition + 1 : currentKeyEndPosition]

		currentValueLen := int(fileData[currentKeyEndPosition])
		if string(currentKey) == (key) {
			return string(fileData[currentKeyEndPosition + 1 : currentKeyEndPosition + 1 + currentValueLen])
		}
		// 2 is the number of bytes for encoding actual key and value lengths
		keysStartPosition += currentKeyLen + currentValueLen + 2
	}

	return ""
}

func GetKeys2(rdbFilePath string) []string {
	var keys []string
	fileData := parseFile(rdbFilePath)
	resizeDbCodePosition := getOpCodePosition(fileData, RESIZE_DB)
	keysLen := getKeysLen(string(fileData), resizeDbCodePosition)

	keysStartPosition := getKeysStartPosition(fileData)

	for i := 0; i < keysLen; i++ {
		currentKeyLen := int(fileData[keysStartPosition])
		currentKeyEndPosition := keysStartPosition + currentKeyLen + 1
		currentKey := fileData[keysStartPosition + 1 : currentKeyEndPosition]

		currentValueLen := int(fileData[currentKeyEndPosition])
		// 2 is the number of bytes for encoding actual key and value lengths
		keysStartPosition += currentKeyLen + currentValueLen + 2

		keys = append(keys, string(currentKey))
	}

	return keys
}

func GetKeys(rdbFilePath string) []string {
	fileData := parseFile(rdbFilePath)
	keysStartPosition := getKeysStartPosition(fileData)
	keyLen := int(fileData[keysStartPosition])
	key := fileData[keysStartPosition + 1 : keysStartPosition + keyLen + 1]

	return []string{ string(key) }
}

// The next byte after the resize db op code
func getKeysLen(fileData string, resizeDbPosition int) int {
	return int(fileData[resizeDbPosition + 1])
}

// keys_len + expire_keys_len + encoding_type + first_key_len
func getKeysStartPosition(fileData []byte) int {
	// 4 is the number of bytes between the fb op and the len of the first key
	return getOpCodePosition(fileData, RESIZE_DB) + 4
}

func getOpCodePosition(fileData []byte, opCode byte) int {
	return bytes.IndexByte(fileData, opCode)
}

func parseFile(filePath string) []byte {
	f, err := os.Open(filePath)
	check(err)

	fileData := make([]byte, 256)
	bytesRead, err := f.Read(fileData)
	check(err)

	return fileData[:bytesRead]
}
