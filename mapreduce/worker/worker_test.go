package worker

import (
	"os"
	"testing"
)

// Worker should read from file
func TestWorkerReadFile(t *testing.T) {
	filepath := "test_resources\\test_worker.json"
	output := readFromFile(filepath)
	if output != "this is a test" {
		t.Errorf("File output = %v; want 'this is a test'", output)
		t.Fail()
	}
}

// Worker should run Map functions
func TestWorkerRunMapFunc(t *testing.T) {

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

}

// Worker should run Reduce functions
func TestWorkerRunReduceFunc(t *testing.T) {

}

// Worker should write to output filesystem
func TestWorkerWriteOutputFilesystem(t *testing.T) {

}
