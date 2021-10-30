package shardkv

import (
	"fmt"
	"github.com/fatih/color"
	"log"
	"os"
)

func init() {
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

const filenamePattern = "test-shardkv-%s-%d.log"

func initGlog(testNum string, gid int) (*log.Logger, *os.File) {
	if testNum == "0" {
		return log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds), nil
	} else {
		filename := fmt.Sprintf(filenamePattern, testNum, gid)
		fd, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			panic(fmt.Sprintf("init log file err %v", err))
		}
		return log.New(fd, "", log.Ldate|log.Ltime|log.Lmicroseconds), fd
	}
}

func (kv *ShardKV) DPrintf(gid, rfme int, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d-%d] ", gid, rfme)
		kv.gLog.Println(colorMap[rfme](prefix+format, a...))
	}
	return
}

func CDPrintf(clientId int64, format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("[%d] ", clientId)
		fmt.Println(color.HiWhiteString(prefix+format, a...))
	}
	return
}
