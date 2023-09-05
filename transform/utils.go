package main

import (
	"encoding/base64"
	"fmt"
	"os"
)

func die(msg string, args ...any) {
	logger.Error(fmt.Sprintf("%+v", args...))
	os.Exit(1)
}

func maybeDie(err error, msg string, args ...any) {
	if err != nil {
		die(msg, args...)
	}
}

// GetEnv Simple helper function to read an environment or return a default value
func getEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func b64DecodeMsg(b64Key string, offsetF ...int) ([]byte, error) {
	offset := 7
	if len(offsetF) > 0 {
		offset = offsetF[0]
	}
	//logger.Debug(fmt.Sprintf("Base64 Encoded String: %s", b64Key))
	var key []byte
	var err error
	if len(b64Key)%4 != 0 {
		key, err = base64.RawStdEncoding.DecodeString(b64Key)
	} else {
		key, err = base64.StdEncoding.DecodeString(b64Key)
	}
	if err != nil {
		return []byte{}, err
	}
	result := key[offset:]
	//logger.Debug(fmt.Sprintf("Base64 Decoded String: %s", result))
	return result, nil
}
