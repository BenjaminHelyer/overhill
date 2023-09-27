package worker

import (
	"os"
	"testing"
)

// Worker should read from file
func TestWorkerReadFile(t *testing.T) {
	filepath := "test_resources\\map_input.json"
	output := ReadFromFile(filepath)
	if output != "this is a test" {
		t.Errorf("File output = %v; want 'this is a test'", output)
		t.Fail()
	}
}

// an example for how we would unit test a Map function by itself
// will likely not use in unit tests for worker process
func MockEmitIntermediate(intermediateKey string, intermediateValue string) {
	return
}

func MockMapFunc_WordCount(filename string, contents string, emit func(string, string)) {
	return
}

// Worker should run Map functions
func TestWorkerRunMapFunc(t *testing.T) {
	input_key := ""
	input_val := ""
	output_key, output_val := RunMapFunc(MockMapFunc_WordCount, input_key, input_val)
	if output_key != "test" || output_val != "test" {
		t.Errorf("Did not receive expected output from running the provided map function.")
		t.Fail()
	}
}

// Worker should write to local disk
func TestWorkerWriteDisk(t *testing.T) {
	filepath := "test_resources\\worker_output.json"
	contents := ""
	WriteToFile(filepath, contents)
	_, err := os.Stat(filepath)
	if err != nil {
		t.Errorf("Error upon attempting to read output file: %v", err)
		t.Fail()
	}
}

// Worker should read from (possibly remote) disk
func TestWorkerReadRemoteDisk(t *testing.T) {
	filepath := "test_resources\\reduce_input.json"
	output := ReadFromFile(filepath)
	if output != "this is a test" {
		t.Errorf("File output = %v; want 'this is a test'", output)
		t.Fail()
	}
}

// an example for how we would unit test a Reduce function by itself
// will likely not use in unit tests for worker process
func MockEmitFinal(outputKey string, outputVals []string) {
	return
}

func MockReduceFunc(string, []string, func(string, []string)) {
	return
}

// Worker should run Reduce functions
func TestWorkerRunReduceFunc(t *testing.T) {
	inputKey := ""
	inputVals := []string{"", "", "", "", "", "", "", "", "", ""}
	outputKey, outputVals := RunReduceFunc(MockReduceFunc, inputKey, inputVals)
	if outputKey != "test" || outputVals != nil {
		t.Errorf("Did not receive expected output from running the provided reduce function.")
		t.Fail()
	}
}

// Worker should write to output filesystem
func TestWorkerWriteOutputFilesystem(t *testing.T) {
	filepath := "test_resources\\worker_output.json"
	contents := ""
	WriteToFile(filepath, contents)
	_, err := os.Stat(filepath)
	if err != nil {
		t.Errorf("Error upon attempting to read output file: %v", err)
		t.Fail()
	}
}
