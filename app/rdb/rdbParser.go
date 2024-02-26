package rdb

import (
	"os"
)

const RESIZE_DB = 0xFB

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func GetKeys(rdbFilePath string) []string {
	f, err := os.Open(rdbFilePath)
	check(err)

	fileData := make([]byte, 256)
	bytesRead, err := f.Read(fileData)
	check(err)

	fileData = fileData[:bytesRead]

	resideDb := byte(RESIZE_DB)
	var resideDbPosition int

	for idx, b := range fileData {
		if b == resideDb {
			resideDbPosition = idx
		}
	}

	keyLenPosition := resideDbPosition + 4
	keyLen := int(fileData[keyLenPosition])
	key := fileData[keyLenPosition : keyLenPosition + keyLen] 

	return []string{ string(key) }
}
