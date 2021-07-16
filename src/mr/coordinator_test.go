package mr

import (
	"container/list"
	"testing"
)

func TestCoordinator_groupAndGenerateReduceTask(t *testing.T) {
	coordinator := Coordinator{
		reduceTasks:              list.New(),
		intermediateFilePathList: []string{"mr-13-1-3", "mr-13-1-4", "mr-13-1-7", "mr-13-1-8", "mr-13-1-9", "mr-13-1-6", "mr-13-1-5", "mr-13-1-0", "mr-13-1-2", "mr-13-1-1", "mr-13-33-1", "mr-13-33-5", "mr-13-33-8", "mr-13-33-2", "mr-13-33-4", "mr-13-33-0", "mr-13-33-6", "mr-13-33-7", "mr-13-33-3", "mr-13-33-9", "mr-13-72-4", "mr-13-72-3", "mr-13-72-8", "mr-13-72-2", "mr-13-72-9", "mr-13-72-6", "mr-13-72-5", "mr-13-72-1", "mr-13-72-7", "mr-13-72-0", "mr-13-66-6", "mr-13-66-0", "mr-13-66-4", "mr-13-66-7", "mr-13-66-2", "mr-13-66-5", "mr-13-66-8", "mr-13-66-1", "mr-13-66-3", "mr-13-66-9", "mr-13-84-2", "mr-13-84-8", "mr-13-84-4", "mr-13-84-3", "mr-13-84-7", "mr-13-84-9", "mr-13-84-1", "mr-13-84-6", "mr-13-84-5", "mr-13-84-0", "mr-13-10-4", "mr-13-10-2", "mr-13-10-9", "mr-13-10-6", "mr-13-10-8", "mr-13-10-1", "mr-13-10-3", "mr-13-10-7", "mr-13-10-0", "mr-13-10-5", "mr-13-54-1", "mr-13-54-0", "mr-13-54-3", "mr-13-54-2", "mr-13-54-6", "mr-13-54-4", "mr-13-54-9", "mr-13-54-8", "mr-13-54-7", "mr-13-54-5", "mr-13-74-1", "mr-13-74-3", "mr-13-74-4", "mr-13-74-9", "mr-13-74-0", "mr-13-74-7", "mr-13-74-2", "mr-13-74-6", "mr-13-74-5", "mr-13-74-8"},
	}
	coordinator.groupAndGenerateReduceTask()
}
