package shardctrler

import (
	"fmt"
	"github.com/fatih/color"
	"log"
	"os"
)

var gLog *log.Logger

func init() {
	gLog = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
	color.NoColor = false
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

func DPrintf(rfme int, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d] ", rfme)
		gLog.Println(colorMap[rfme](prefix+format, a...))
	}
	return
}

func CDPrintf(clientId int64, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d] ", clientId)
		gLog.Println(color.HiWhiteString(prefix+format, a...))
	}
	return
}

func TPrintf(rfme int, format string, a ...interface{}) {
	if Trace {
		prefix := fmt.Sprintf("[%d] ", rfme)
		gLog.Println(colorMap[rfme](prefix+format, a...))
	}
}
