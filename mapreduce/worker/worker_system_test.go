package worker

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

func TestRunMapFunc_WordCount(t *testing.T) {
	inputFile := "test_resources/map_input.txt"
	mapFuncKey := "wc"
	expectedOutputFilepath := "intermediate.txt"
	expectedWordCount := 9

	var uutWorker Worker
	uutWorker.RunMapProcess(inputFile, mapFuncKey)

	print("Emitted key/val are: \n")
	print(uutWorker.emittedIntermediateKeys)
	print(uutWorker.emittedIntermediateVals)

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

	fileText := ""

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		currLine := scanner.Text()
		fileText += currLine
	}

	kvPairs := strings.Split(fileText, " ! ")

	for _, line := range kvPairs {
		print(line)
		print("\n*****\n")
	}

	wordCount := 0
	for _, line := range kvPairs {
		pair := strings.Split(line, ": ")
		if pair[0] != "" && pair[1] != "1" {
			t.Errorf("Found intermediate value %v other than '1' in word count Map example.", pair[1])
			t.Fail()
		}
		wordCount++
	}

	if wordCount != expectedWordCount {
		t.Errorf("WordCount was %v, which differed from expected word count of %v.", wordCount, expectedWordCount)
		t.Fail()
	}

	// if fileText != contents {
	// 	t.Errorf("Read text does not match input contents. Input contents were: %v", contents)
	// 	t.Fail()
	// }

	file.Close()
	// os.Remove(expectedOutputFilepath)
	// if err != nil {
	// 	t.Errorf("Error deleting")
	// }
}
