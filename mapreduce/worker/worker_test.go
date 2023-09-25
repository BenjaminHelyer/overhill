package worker

import "testing"

// Worker should read from file
func TestWorkerReadFile(t *testing.T) {
	filename := "test_worker.json"
	output := readFromFile(filename)
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