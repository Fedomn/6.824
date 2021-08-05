package raft

import "log"

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
