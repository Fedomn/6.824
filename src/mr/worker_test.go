package mr

import (
	"bufio"
	"encoding/json"
	"strings"
	"testing"
)

func TestWorker_handleReduceTask(t *testing.T) {
	var content = `{"Key":"Complete","Value":"1"}
{"Key":"no","Value":"1"}
{"Key":"cost","Value":"1"}
{"Key":"no","Value":"1"}
{"Key":"restrictions","Value":"1"}
{"Key":"or","Value":"1"}
{"Key":"or","Value":"1"}
{"Key":"www","Value":"1"}
{"Key":"net","Value":"1"}`

	intermediateKV := make([]KeyValue, 0)

	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		kv := KeyValue{}
		text := scanner.Text()
		decoder := json.NewDecoder(strings.NewReader(text))
		if err := decoder.Decode(&kv); err != nil {
			t.Log(err)
		}
		intermediateKV = append(intermediateKV, kv)
	}

	t.Logf("%v", intermediateKV)

}
