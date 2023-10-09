package systemTest

import (
	"encoding/json"
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
	coordConfigFile := "../coordinator/test_resources/single_mocked_worker.json"
	emersonFolder := "test_storage"

	expectedFinalOutput := map[string]string{"test": "test"}

	var wg sync.WaitGroup

	// TODO: ensure the worker process is closed
	workerArgs := []string{"--port=5050"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		workerProcess, workerError := runSubprocess("../main.exe", workerArgs)
		defer workerProcess.Process.Kill()
		if workerError != nil {
			t.Errorf("Error running worker subprocess: %v", workerError)
			t.Fail()
		}
	}()

	coordinatorArgs := []string{"--coord", "--config=" + coordConfigFile, "--mapFunc=wc_total", "--reduceFunc=wc_total", "--input=" + emersonFolder}
	wg.Add(1)
	go func() {
		defer wg.Done()
		coordProcess, coordError := runSubprocess("../main.exe", coordinatorArgs)
		defer coordProcess.Process.Kill()
		if coordError != nil {
			t.Errorf("Error running coordinator subprocess: %v", coordError)
			t.Fail()
		}
	}()

	wg.Wait()

	expectedFinalResultPath := "test_final.json"
	// expectedIntermediateResultsPath := "intermediate/test_intermediate.json"

	_, fileExistsError := os.Stat(expectedFinalResultPath)
	if fileExistsError != nil {
		t.Errorf("Final output file does not exist: %v", fileExistsError)
		t.Fail()
	}

	finalFile, fileOpenError := os.Open(expectedFinalResultPath)
	if fileOpenError != nil {
		t.Errorf("Error upon opening final output file: %v", fileOpenError)
		t.Fail()
	}

	finalOutput := make(map[string]string)
	decoder := json.NewDecoder(finalFile)
	if decodeErr := decoder.Decode(&finalOutput); decodeErr != nil {
		t.Errorf("Error upon decoding output file: %v", decodeErr)
		t.Fail()
	}

	for index, val := range expectedFinalOutput {
		if finalOutput[index] != val {
			t.Errorf("Final output does not match expected on a given key: %v, found values %v != %v", index, val, finalOutput[index])
			t.Fail()
		}
	}

	// finalFile.Close()
	// removeErr := os.Remove(expectedFinalResultPath)
	// if removeErr != nil {
	// 	t.Errorf("Error deleting final output")
	// }

	// os.Remove(expectedIntermediateResultsPath)
	// if removeErr != nil {
	// 	t.Errorf("Error deleting intermediate output")
	// }
}

func runSubprocess(name string, args []string) (*exec.Cmd, error) {
	print("***\n")
	print("Running subprocess: ", name, " with args: ", args, "\n")
	print("***\n")
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if startErr := cmd.Start(); startErr != nil {
		return nil, startErr
	}

	return cmd, nil
}
