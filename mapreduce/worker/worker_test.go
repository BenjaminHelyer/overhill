package worker

import (
	"os"
	"testing"
)

// Worker should read from file
func TestWorkerReadFile(t *testing.T) {
	filepath := "test_resources\\map_input.json"
	output := readFromFile(filepath)
	if output != "this is a test" {
		t.Errorf("File output = %v; want 'this is a test'", output)
		t.Fail()
	}
}

func MockMapFunc(string, string) (string, string) {
	return "", ""
}

// Worker should run Map functions
func TestWorkerRunMapFunc(t *testing.T) {
	input_key := ""
	input_val := ""
	output_key, output_val := runMapFunc(MockMapFunc, input_key, input_val)
	if output_key != "test" || output_val != "test" {
		t.Errorf("Did not receive expected output from running the provided map function.")
		t.Fail()
	}
}

// Worker should write to local disk
func TestWorkerWriteDisk(t *testing.T) {
	filepath := "test_resources\\worker_output.json"
	writeToFile(filepath)
	_, err := os.Stat(filepath)
	if err != nil {
		t.Errorf("Error upon attempting to read output file: %v", err)
		t.Fail()
	}
}

// Worker should read from (possibly remote) disk
func TestWorkerReadRemoteDisk(t *testing.T) {
	filepath := "test_resources\\reduce_input.json"
	output := readFromFile(filepath)
	if output != "this is a test" {
		t.Errorf("File output = %v; want 'this is a test'", output)
		t.Fail()
	}
}

func MockReduceFunc(string, [10]string) (string, [2]string) {
	return "", [2]string{"", ""}
}

// Worker should run Reduce functions
func TestWorkerRunReduceFunc(t *testing.T) {
	input_key := ""
	input_vals := [10]string{"", "", "", "", "", "", "", "", "", ""}
	output_key, output_vals := runReduceFunc(MockReduceFunc, input_key, input_vals)
	if output_key != "test" || output_vals != [2]string{"test", "test"} {
		t.Errorf("Did not receive expected output from running the provided reduce function.")
		t.Fail()
	}
}

// Worker should write to output filesystem
func TestWorkerWriteOutputFilesystem(t *testing.T) {
	filepath := "test_resources\\worker_output.json"
	writeToFile(filepath)
	_, err := os.Stat(filepath)
	if err != nil {
		t.Errorf("Error upon attempting to read output file: %v", err)
		t.Fail()
	}
}
