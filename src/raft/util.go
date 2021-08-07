package raft

import (
	"fmt"
	"log"
	"os"
)

var gLog *log.Logger

func init() {
	gLog = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
}

// Debugging
const Debug = true

func DPrintf(rfme int, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d] ", rfme)
		gLog.Printf(prefix+format, a...)
	}
	return
}
