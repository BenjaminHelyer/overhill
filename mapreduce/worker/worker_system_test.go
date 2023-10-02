package worker

import (
	"encoding/json"
	"os"
	"testing"
)

func TestRunMapFunc_WordCount(t *testing.T) {
	inputFile := "test_resources/map_input.txt"
	mapFuncKey := "wc"
	intermediateFilename := "intermediate.json"
	expectedWordCount := 9

	var uutWorker Worker
	uutWorker.RunMapProcess(inputFile, mapFuncKey, intermediateFilename)

	_, err := os.Stat(intermediateFilename)
	if err != nil {
		t.Errorf("Intermediate file does not exist after running Map process: %v", err)
		t.Fail()
	}

	file, fileOpenError := os.Open(intermediateFilename)
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
	os.Remove(intermediateFilename)
}

func TestRunReduceFunc_WordCount(t *testing.T) {
	inputFile := "test_resources/reduce_input.json"
	reduceFuncKey := "wc"
	finalFilename := "final.json"

	var uutWorker Worker
	uutWorker.RunReduceProcess(inputFile, reduceFuncKey, finalFilename)

	_, err := os.Stat(finalFilename)
	if err != nil {
		t.Errorf("Final file does not exist after running Map process: %v", err)
		t.Fail()
	}

	file, fileOpenError := os.Open(finalFilename)
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

	for _, kv := range decodedData {
		if kv.Key == "the" {
			if kv.Value != "2" {
				t.Errorf("Got different value than expected for 'the', got %v, expected %v.", kv.Value, 2)
			}
		}
	}

	file.Close()
	os.Remove(finalFilename)
}

func TestRunMapAndReduce_WordCount_Total(t *testing.T) {
	inputFile := "test_resources/map_input.txt"
	funcKey := "wc_total"
	intermediateFilename := "intermediate_2.json"
	finalFilename := "final.json"

	var uutWorker Worker

	uutWorker.RunMapProcess(inputFile, funcKey, intermediateFilename)
	uutWorker.RunReduceProcess(intermediateFilename, funcKey, finalFilename)

	_, intermediateFileError := os.Stat(intermediateFilename)
	if intermediateFileError != nil {
		t.Errorf("Intermediate file does not exist after running Map process: %v", intermediateFileError)
		t.Fail()
	}

	intermediateFile, intermediateOpenError := os.Open(intermediateFilename)
	if intermediateOpenError != nil {
		t.Errorf("Error upon opening intermediate file.")
		t.Fail()
	}

	_, finalFileError := os.Stat(finalFilename)
	if finalFileError != nil {
		t.Errorf("Final file does not exist after running Map process: %v", finalFileError)
		t.Fail()
	}

	file, fileOpenError := os.Open(finalFilename)
	if fileOpenError != nil {
		t.Errorf("Error upon opening final file.")
		t.Fail()
	}

	var decodedData []KeyValue
	decoder := json.NewDecoder(file)

	if decodeErr := decoder.Decode(&decodedData); decodeErr != nil {
		t.Errorf("Error upon decoding output file.")
		t.Fail()
	}

	for _, kv := range decodedData {
		if kv.Key != "word" {
			t.Errorf("Expected only one key for total word count, got %v, expected %v.", kv.Key, "word")
			t.Fail()
		} else if kv.Value != "9" {
			t.Errorf("Expected different total word count, got %v, expected %v.", kv.Value, "9")
			t.Fail()
		}
	}

	file.Close()
	intermediateFile.Close()

	removeIntermdiateErr := os.Remove(intermediateFilename)
	if removeIntermdiateErr != nil {
		t.Errorf("Received an error upon deleting the intermediate file: %v", removeIntermdiateErr)
		t.Fail()
	}

	removeFinalErr := os.Remove(finalFilename)
	if removeFinalErr != nil {
		t.Errorf("Received an error upon deleting the final file: %v", removeFinalErr)
		t.Fail()
	}
}
