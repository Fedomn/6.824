package raft

import (
	"fmt"
	"github.com/fatih/color"
	"log"
	"os"
)

func init() {
	color.NoColor = false
}

func initGlog(glog *log.Logger) *log.Logger {
	if glog == nil {
		return log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	return glog
}

var colorMap = map[int]func(format string, a ...interface{}) string{
	0: color.RedString,
	1: color.GreenString,
	2: color.YellowString,
	3: color.BlueString,
	4: color.MagentaString,
	5: color.CyanString,
	6: color.WhiteString,

	7:  color.HiRedString,
	8:  color.HiGreenString,
	9:  color.HiYellowString,
	10: color.HiBlueString,
	11: color.HiMagentaString,
	12: color.HiCyanString,
	13: color.HiWhiteString,
}

// Debugging
const Debug = true
const Trace = false

func (rf *Raft) SetGlog(glog *log.Logger) {
	rf.gLog = glog
}

func (rf *Raft) DPrintf(rfme int, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d] ", rfme)
		rf.gLog.Println(colorMap[rfme](prefix+format, a...))
	}
	return
}

func (rf *Raft) DRpcPrintf(rfme int, seq uint32, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d] [Seq%d] ", rfme, seq)
		rf.gLog.Println(colorMap[rfme](prefix+format, a...))
	}
	return
}

func (rf *Raft) TRpcPrintf(rfme int, seq uint32, format string, a ...interface{}) {
	if Trace {
		prefix := fmt.Sprintf("[%d] [Seq%d] ", rfme, seq)
		rf.gLog.Println(colorMap[rfme](prefix+format, a...))
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
