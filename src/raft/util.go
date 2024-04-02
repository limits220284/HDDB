package raft

import (
	"fmt"
	"log"
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

type Timer struct {
	elapsedTime time.Duration
	lastTime    time.Time
}

func (t *Timer) String() string {
	return fmt.Sprintf("{elapsedTime:%v}", t.elapsedTime)
}

func (t *Timer) elapsed() {
	t.elapsedTime += time.Now().Sub(t.lastTime)
	t.lastTime = time.Now()
}

func (t *Timer) isTimeOut(timeOut time.Duration) bool {
	return t.elapsedTime > timeOut
}

func (t *Timer) reset() {
	t.elapsedTime = 0
	t.lastTime = time.Now()
}
