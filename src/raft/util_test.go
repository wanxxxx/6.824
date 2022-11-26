package raft

import (
	"fmt"
	"testing"
	"time"
)

func Test_getRandTime(t *testing.T) {
	go func() {
		fmt.Printf("1: %d\n", getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS))
	}()
	go func() {
		fmt.Printf("2: %d\n", getRandTime(MIN_ElECTION_MS, MAX_ElECTION_MS))
	}()
	time.Sleep(time.Second * 5)
}
