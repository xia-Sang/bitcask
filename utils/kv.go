package utils

import (
	"fmt"
	"math/rand"
	"time"
)

const letterBytes = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func GenerateKey(index int) []byte {
	return []byte(fmt.Sprintf("key-%09d", index))
}

func GenerateValue(length int) []byte {
	rand.Seed(time.Now().UnixNano())
	value := make([]byte, length)
	for i := range value {
		value[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return value
}
