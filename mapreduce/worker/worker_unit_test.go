package worker

import (
	"bufio"
	"os"
	"testing"
)

// Read file function should raise error upon nonexistent file
func TestWorkerReadFile_Nonexistent(t *testing.T) {
	filepath := "nonexistent\\garbage\\nihil.txt"
	output, err := ReadFromFile(filepath)
	if err == nil {
		t.Errorf("Error not raised upon reasing from nonexistent file.")
		t.Fail()
	}
	if output != "" {
		t.Errorf("Got nonempty output from nonexistent file.")
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

// Worker should write to local disk
func TestWorkerWriteDisk(t *testing.T) {
	filepath := "test_resources\\worker_output.txt"
	contents := "the lazy dog jumped over the quick brown fox"
	writerError := WriteToFile(filepath, contents)

	if writerError != nil {
		t.Errorf("Error upon calling function to write to file: %v", writerError)
		t.Fail()
	}

	_, err := os.Stat(filepath)
	if err != nil {
		t.Errorf("Output file does not exist: %v", err)
		t.Fail()
	}

	file, fileOpenError := os.Open(filepath)
	if fileOpenError != nil {
		t.Errorf("Error upon opening output file.")
		t.Fail()
	}

	fileText := ""

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		currLine := scanner.Text()
		fileText += currLine
	}

	if fileText != contents {
		t.Errorf("Read text does not match input contents. Input contents were: %v", contents)
		t.Fail()
	}

	file.Close()
	os.Remove(filepath)
	if err != nil {
		t.Errorf("Error deleting")
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

	os.Remove(filepath)
}

func TestWorkerEmitIntermediate(t *testing.T) {
	var uutWorker Worker

	key1 := "test"
	val1 := "123"

	uutWorker.EmitIntermediate(key1, val1)

	for _, intermediatePair := range uutWorker.emitttedIntermediates {
		if intermediatePair.Key != "test" {
			t.Errorf("Different key found than expected. Key was %v, anticipated 'test'.", intermediatePair.Key)
			t.Fail()
		}
		if intermediatePair.Value != "123" {
			t.Errorf("Different value found than expected. Key was %v, anticipated '123'.", intermediatePair.Key)
			t.Fail()
		}
	}
}
