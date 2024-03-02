package rdb

import (
	"bytes"
	"encoding/binary"
	"os"
	"time"
)

const RESIZE_DB = 0xFB
const DB_END = 0xFF
const SECONDS_EXPIRY = 0xFD
const MS_EXPIRY = 0xFC

type keyValue struct {
	key         string
	value       string
	totalLength int
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func GetKeyValue(rdbFilePath string, key string) string {
	fileData := parseFile(rdbFilePath)
	keysLen := getKeysLen(fileData)
	expiryKeysLen := getExpiryKeysLen(fileData)

	firstKeyLenIdx := getKeysStartIdx(fileData)
	for i := 0; i < expiryKeysLen; i++ {
		keyValue, expired := parseExpiryKeyValue(fileData, firstKeyLenIdx)
		if keyValue.key == key && expired {
			return ""
		}
		if keyValue.key == key {
			return keyValue.value
		}

	}

	for i := 0; i < keysLen; i++ {
		keyValue := parseKeyValue(fileData, firstKeyLenIdx)
		if keyValue.key == key {
			return keyValue.value
		}

		// 3: key_len + value_len + encoding
		firstKeyLenIdx += keyValue.totalLength + 3
	}

	return ""
}

func GetKeys(rdbFilePath string) []string {
	var keys []string
	fileData := parseFile(rdbFilePath)
	keysLen := getKeysLen(fileData)

	firstKeyLenIdx := getKeysStartIdx(fileData)

	for i := 0; i < keysLen; i++ {
		keyValue := parseKeyValue(fileData, firstKeyLenIdx)
		keys = append(keys, keyValue.key)

		// 3: key_len + value_len + encoding
		firstKeyLenIdx += keyValue.totalLength + 3
	}

	return keys
}

func parseExpiryKeyValue(fileData []byte, currIdx int) (keyValue, bool) {
	pairExpiryType := fileData[currIdx]
	var expirationValueEndIdx int
	expired := false
	if pairExpiryType == SECONDS_EXPIRY {
		expirationValueEndIdx = currIdx + 4
		expirationTime := binary.LittleEndian.Uint64(fileData[currIdx:expirationValueEndIdx])
		if int64(expirationTime) < time.Now().Unix() {
			expired = true
		}
	} else {
		expirationValueEndIdx = currIdx + 8
		expirationTime := binary.LittleEndian.Uint64(fileData[currIdx:expirationValueEndIdx])
		if int64(expirationTime) < time.Now().UnixMilli() {
			expired = true
		}
	}
	keyValue := parseKeyValue(fileData, expirationValueEndIdx+1)
	return keyValue, expired
}

func parseKeyValue(fileData []byte, currIdx int) keyValue {
	keyLen := int(fileData[currIdx])
	keyEndIdx := currIdx + keyLen + 1
	key := string(fileData[currIdx+1 : keyEndIdx])

	valueLen := int(fileData[keyEndIdx])
	value := string(fileData[keyEndIdx+1 : keyEndIdx+1+valueLen])
	return keyValue{key, value, len(key) + len(value)}
}

// 1: -> The next byte after the resize db op code
func getKeysLen(fileData []byte) int {
	return int(fileData[getOpCodeIdx(fileData, RESIZE_DB)+1])
}

// 2: -> The second byte after the resize db op code
func getExpiryKeysLen(fileData []byte) int {
	return int(fileData[getOpCodeIdx(fileData, RESIZE_DB)+2])
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
