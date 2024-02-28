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
	keysLen := getKeysLen(fileData)

	firstKeyLenIdx := getKeysStartIdx(fileData)
	for i := 0; i < keysLen; i++ {
		currentKey, value := parseKeyValue(fileData, firstKeyLenIdx)
		if currentKey == key {
			return value
		}

		// 3: key_len + value_len + encoding
		firstKeyLenIdx += len(currentKey) + len(value) + 3
	}

	return ""
}

func GetKeys(rdbFilePath string) []string {
	var keys []string
	fileData := parseFile(rdbFilePath)
	keysLen := getKeysLen(fileData)

	firstKeyLenIdx := getKeysStartIdx(fileData)
	for i := 0; i < keysLen; i++ {
		key, value := parseKeyValue(fileData, firstKeyLenIdx)
		keys = append(keys, key)

		// 3: key_len + value_len + encoding
		firstKeyLenIdx += len(key) + len(value) + 3
	}

	return keys
}

func parseKeyValue(fileData []byte, keyLenIdx int) (string, string) {
	keyLen := int(fileData[keyLenIdx])
	keyEndIdx := keyLenIdx + keyLen + 1
	key := string(fileData[keyLenIdx + 1 : keyEndIdx])

	valueLen := int(fileData[keyEndIdx])
	value := string(fileData[keyEndIdx + 1 : keyEndIdx + 1 + valueLen])
	return key, value
}

// 1: -> The next byte after the resize db op code
func getKeysLen(fileData []byte) int {
	return int(fileData[getOpCodeIdx(fileData, RESIZE_DB) + 1])
}

// 4: keys_len + expire_keys_len + encoding_type + first_key_len
func getKeysStartIdx(fileData []byte) int {
	return getOpCodeIdx(fileData, RESIZE_DB) + 4
}

func getOpCodeIdx(fileData []byte, opCode byte) int {
	return bytes.IndexByte(fileData, opCode)
}

func parseFile(filePath string) []byte {
	f, err := os.Open(filePath)
	check(err)

	fileData := make([]byte, 1024)
	bytesRead, err := f.Read(fileData)
	check(err)

	return fileData[:bytesRead]
}
