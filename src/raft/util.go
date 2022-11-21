package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func MaxInt(num1, num2 int) int {
	if num1 > num2 {
		return num1
	}
	return num2
}

func MinInt(num1, num2 int) int {
	if num1 > num2 {
		return num2
	}
	return num1
}

func getRandTime(start, end int) time.Duration {
	rand.Seed(time.Now().UnixNano())
	return TIME_UNIT * time.Duration(rand.Intn(end-start)+start)
}
