package systemTest

import (
	"os"
	"os/exec"
	"sync"
	"testing"
)

// we shall get the total word count of
// several works of Emerson via MapReduce;
// the works are kept in the storage folder
// and distributed among the workers for
// the Map functions.
// For now only one worker will run a Reduce
// function (we don't worry about partitioning
// for Reduce right now)
func TestEmersonWordCount_SingleWorker(t *testing.T) {
	// coordConfigFile := "../coordinator/test_resources/single_mocked_worker.json"
	// emersonFolder := "../../test_storage/emerson"

	var wg sync.WaitGroup

	workerArgs := []string{"--port=5050"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runSubprocess("../main", workerArgs); err != nil {
			t.Errorf("Error running worker subprocess: %v", err)
			t.Fail()
		}
	}()

	coordinatorArgs := []string{"--coord", "--config=\"coordinator/test_resources/hardcoded_workers.json\"", "--mapFunc=wc_total", "--reduceFunc=wc_total", "--input=/system_test/test_storage/"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runSubprocess("../main", coordinatorArgs); err != nil {
			t.Errorf("Error running coordinator subprocess: %v", err)
			t.Fail()
		}
	}()
}

func runSubprocess(name string, args []string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
