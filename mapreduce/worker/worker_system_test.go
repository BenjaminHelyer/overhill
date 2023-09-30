package worker

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRunMapFunc_WordCount(t *testing.T) {
	inputFile := "test_resources/map_input.txt"
	mapFuncKey := "wc"
	expectedOutputFilepath := "intermediate.json"
	expectedWordCount := 9

	var uutWorker Worker
	uutWorker.RunMapProcess(inputFile, mapFuncKey)

	_, err := os.Stat(expectedOutputFilepath)
	if err != nil {
		t.Errorf("Intermediate file does not exist after running Map process: %v", err)
		t.Fail()
	}

	file, fileOpenError := os.Open(expectedOutputFilepath)
	if fileOpenError != nil {
		t.Errorf("Error upon opening output file.")
		t.Fail()
	}

	var decodedData []KeyValue
	decoder := json.NewDecoder(file)

	if decodeErr := decoder.Decode(&decodedData); decodeErr != nil {
		t.Errorf("Error upon decoding output file.")
		t.Fail()
	}

	wordCount := 0
	for _, kv := range decodedData {
		if kv.Value != "1" {
			t.Errorf("Found intermediate value %v in word count example; all values in this example anticipated to be 1.", kv.Value)
			t.Fail()
		}
		wordCount++
	}

	if wordCount != expectedWordCount {
		t.Errorf("Word count found was %v, expected word count was %v.", wordCount, expectedWordCount)
		t.Fail()
	}

	file.Close()
	os.Remove(expectedOutputFilepath)
}

func TestRunReduceFunc_WordCount(t *testing.T) {
	inputFile := "test_resources/reduce_input.json"
	reduceFuncKey := "wc"
	expectedOutputFilepath := "final.json"

	var uutWorker Worker
	uutWorker.RunReduceProcess(inputFile, reduceFuncKey)

	_, err := os.Stat(expectedOutputFilepath)
	if err != nil {
		t.Errorf("Intermediate file does not exist after running Map process: %v", err)
		t.Fail()
	}

	file, fileOpenError := os.Open(expectedOutputFilepath)
	if fileOpenError != nil {
		t.Errorf("Error upon opening output file.")
		t.Fail()
	}

	var decodedData []KeyValue
	decoder := json.NewDecoder(file)

	print(decodedData)
	print("*****")
	print(decoder)

}
