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

func (log1 *LogEntry) compare(log2 *LogEntry) int {

	return compareLog(log1.Term, log2.Term, log1.Index, log2.Index)
}

func (log1 *LogEntry) equals(log2 *LogEntry) bool {
	if log1 == nil || log2 == nil {
		return false
	}
	return log1.Term == log2.Term && log1.Index == log2.Index
}

func compareLog(term1, term2 int, index1, index2 int) int {
	if term1 != term2 {
		return term1 - term2
	}
	return int(index1 - index2)
}

func compareLogs(e1, e2 []*LogEntry) bool {
	if len(e1) != len(e2) {
		return false
	}
	for i := 0; i < len(e1); i++ {
		if !e1[i].equals(e2[i]) {
			return false
		}
	}
	return true
}

func compareArr(e1, e2 []int) bool {
	if len(e1) != len(e2) {
		return false
	}
	for i := 0; i < len(e1); i++ {
		if e1[i] != (e2[i]) {
			return false
		}
	}
	return true
}
