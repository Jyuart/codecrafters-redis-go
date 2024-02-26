package rdb

import (
	"bytes"
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
	resideDbPosition := bytes.IndexByte(fileData, byte(RESIZE_DB))

	// 4 is the number of bytes between the fb op and the len of the first key
	keyLenPosition := resideDbPosition + 4
	keyLen := int(fileData[keyLenPosition])
	key := fileData[keyLenPosition + 1 : keyLenPosition + keyLen + 1]

	return []string{ string(key) }
}
