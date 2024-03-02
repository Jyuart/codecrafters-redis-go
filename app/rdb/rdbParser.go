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

func GetKeyValue(rdbFilePath string, key string) (string, bool) {
	fileData := parseFile(rdbFilePath)
	keysLen := getKeysLen(fileData)

	currIdx := getPairsStartIdx(fileData)
	for i := 0; i < keysLen; i++ {
		keyValue, expired := parseKeyValue(fileData, currIdx)
		if keyValue.key == key {
			return keyValue.value, expired
		}

		currIdx += keyValue.totalLength + 3
	}

	return "", false
}

func GetKeys(rdbFilePath string) []string {
	var keys []string
	fileData := parseFile(rdbFilePath)
	keysLen := getKeysLen(fileData)

	currIdx := getPairsStartIdx(fileData)
	for i := 0; i < keysLen; i++ {
		keyValue, expired := parseKeyValue(fileData, currIdx)
		if !expired {
			keys = append(keys, keyValue.key)
		}

		currIdx += keyValue.totalLength
	}

	return keys
}

func parseKeyValue(fileData []byte, currIdx int) (keyValue, bool) {
	firstByte := fileData[currIdx]
	expired := false
	var currIdxOffset int

	if firstByte == SECONDS_EXPIRY || firstByte == MS_EXPIRY {
		if firstByte == SECONDS_EXPIRY {
			expirationTime := binary.LittleEndian.Uint64(fileData[currIdx+1 : currIdx+5])
			if int64(expirationTime) < time.Now().Unix() {
				expired = true
				// expire type + 4 bytes for value
				currIdxOffset = 5
			}
		} else {
			expirationTime := binary.LittleEndian.Uint64(fileData[currIdx+1 : currIdx+9])
			if int64(expirationTime) < time.Now().UnixMilli() {
				expired = true
				// expire type + 8 bytes for value
				currIdxOffset = 9
			}
		}
	}

	keyValue := parseKeyValueInner(fileData, currIdx+currIdxOffset)
	keyValue.totalLength += currIdxOffset
	return keyValue, expired
}

func parseKeyValueInner(fileData []byte, currIdx int) keyValue {
	// 1 because currIdx points to encoding
	keyLen := int(fileData[currIdx+1])
	// 1 to skip encoding, 1 to include key ending
	keyEndIdx := currIdx + 1 + keyLen + 1
	// 2 to skip encoding and len
	key := string(fileData[currIdx+2 : keyEndIdx])

	valueLen := int(fileData[keyEndIdx])
	value := string(fileData[keyEndIdx+1 : keyEndIdx+1+valueLen])

	// 3 for encoding, key len, value len
	return keyValue{key, value, len(key) + len(value) + 3}
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
func getPairsStartIdx(fileData []byte) int {
	return getOpCodeIdx(fileData, RESIZE_DB) + 3
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
