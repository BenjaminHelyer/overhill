package systemTest

import (
	"encoding/json"
	"main/worker"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
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
	emersonFolder := "test_storage/self_reliance/"

	expectedFinalOutput := []worker.KeyValue{
		{"word", "9985"},
	}

	// timeout to ensure the test doesn't run forever
	timeout := 2 * time.Second
	t.Parallel() // allow this to run in parallel with other tests

	timeoutCh := make(chan struct{})
	go func() {
		time.Sleep(timeout + 1)
		timeoutCh <- struct{}{}
	}()

	var wg sync.WaitGroup
	var workerProcess, coordProcess *exec.Cmd
	var workerError, coordError error
	// need to ensure the worker process starts before the coordinator
	workerStarted := make(chan struct{})

	workerArgs := []string{"--port=5050"}
	wg.Add(1)
	go func() {
		workerProcess, workerError = runSubprocess("../main.exe", workerArgs)
		if workerError != nil {
			t.Errorf("Error running worker subprocess: %v", workerError)
			t.Fail()
		}

		close(workerStarted)
		defer wg.Done()
	}()

	// wait for the worker process to start
	<-workerStarted

	coordinatorArgs := []string{"--coord", "--config=" + coordConfigFile, "--mapFunc=wc_total", "--reduceFunc=wc_total", "--input=" + emersonFolder}
	wg.Add(1)
	go func() {
		coordProcess, coordError = runSubprocess("../main.exe", coordinatorArgs)
		if coordError != nil {
			t.Errorf("Error running coordinator subprocess: %v", coordError)
			t.Fail()
		}

		coordError := coordProcess.Wait()
		if coordError != nil {
			t.Errorf("Error running coordinator subprocess: %v", coordError)
			t.Fail()
		}

		defer wg.Done()
	}()

	wg.Wait()

	expectedFinalResultPath := "test_final.json"
	expectedIntermediateResultsPath := "intermediate/test_intermediate.json"

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

	var finalOutput []worker.KeyValue
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

	finalFile.Close()
	removeErr := os.Remove(expectedFinalResultPath)
	if removeErr != nil {
		t.Errorf("Error deleting final output")
	}

	os.Remove(expectedIntermediateResultsPath)
	if removeErr != nil {
		t.Errorf("Error deleting intermediate output")
	}

	defer func() {
		if workerProcess != nil && workerProcess.Process != nil {
			print("Killing worker process...\n")
			killErr := workerProcess.Process.Kill()
			if killErr != nil {
				t.Errorf("Error upon trying to kill worker process: %v", killErr)
				t.Fail()
			}
		}
	}()

	select {
	case <-timeoutCh:
		// test completed within timeout bounds
		print("Test completed within timeout bounds.\n")
	case <-time.After(timeout):
		t.Errorf("Test timed out after %v [s]", timeout)
		t.Fail()
	}
}

func TestEmersonWordCount_MultiWorker(t *testing.T) {
	coordConfigFile := "../coordinator/test_resources/hardcoded_workers.json"
	emersonFolder := "test_storage/emerson/"

	expectedFinalOutput := []worker.KeyValue{
		{"word", "39473"},
	}

	// timeout to ensure the test doesn't run forever
	timeout := 2 * time.Second
	t.Parallel() // allow this to run in parallel with other tests

	timeoutCh := make(chan struct{})
	go func() {
		time.Sleep(timeout + 1)
		timeoutCh <- struct{}{}
	}()

	var wg sync.WaitGroup
	var workerProcesses []*exec.Cmd
	var coordProcess *exec.Cmd
	var coordError error

	workerArgs := [][]string{{"--port=5060"}, {"--port=5070"}, {"--port=5080"}}
	// need to ensure the worker process starts before the coordinator
	workerStarted := make([]chan struct{}, len(workerArgs))
	for i := range workerStarted {
		workerStarted[i] = make(chan struct{})
	}

	for i, arg := range workerArgs {
		go func(i int, arg []string) {
			workerProcess, workerError := runSubprocess("../main.exe", arg)
			if workerError != nil {
				t.Errorf("Error running worker subprocess: %v", workerError)
				t.Fail()
			}
			workerProcesses = append(workerProcesses, workerProcess)
			close(workerStarted[i])
		}(i, arg)
	}

	// wait for all worker processes to start
	for i := range workerStarted {
		<-workerStarted[i]
	}

	coordinatorArgs := []string{"--coord", "--config=" + coordConfigFile, "--mapFunc=wc_total", "--reduceFunc=wc_total", "--input=" + emersonFolder}
	wg.Add(1)
	go func() {
		coordProcess, coordError = runSubprocess("../main.exe", coordinatorArgs)
		if coordError != nil {
			t.Errorf("Error running coordinator subprocess: %v", coordError)
			t.Fail()
		}

		coordError := coordProcess.Wait()
		if coordError != nil {
			t.Errorf("Error running coordinator subprocess: %v", coordError)
			t.Fail()
		}

		defer wg.Done()
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

	var finalOutput []worker.KeyValue
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

	// removeIntermediateErr := os.Remove(expectedIntermediateResultsPath)
	// if removeIntermediateErr != nil {
	// 	t.Errorf("Error deleting intermediate output")
	// }

	defer func() {
		for _, workerProcess := range workerProcesses {
			if workerProcess != nil && workerProcess.Process != nil {
				print("Killing worker processes...\n")
				killErr := workerProcess.Process.Kill()
				if killErr != nil {
					t.Errorf("Error upon trying to kill worker process: %v", killErr)
					t.Fail()
				}
			}
		}
	}()

	select {
	case <-timeoutCh:
		// test completed within timeout bounds
		print("Test completed within timeout bounds.\n")
	case <-time.After(timeout):
		t.Errorf("Test timed out after %v [s]", timeout)
		t.Fail()
	}
}

func runSubprocess(name string, args []string) (*exec.Cmd, error) {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if startErr := cmd.Start(); startErr != nil {
		return nil, startErr
	}

	return cmd, nil
}
