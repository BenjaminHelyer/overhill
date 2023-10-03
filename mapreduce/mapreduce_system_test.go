package main

import (
	"encoding/json"
	"main/coordinator"
	"os"
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
func TestEmersonWordCount(t *testing.T) {
	configFile := "test_resources/mocked_workers.json"
	emersonFolder := "test_resources/emerson/"
	var uutCoordinator coordinator.Coordinator
	finalOutput, coordErrors := uutCoordinator.RunCoordinator(configFile, "wc_total", emersonFolder)

	if coordErrors != nil {
		t.Errorf("Encountered errors when running the Coordinator: %v", coordErrors)
		t.Fail()
	}

	_, err := os.Stat(finalOutput)
	if err != nil {
		t.Errorf("Final file does not exist after running Map process: %v", err)
		t.Fail()
	}

	file, fileOpenError := os.Open(finalOutput)
	if fileOpenError != nil {
		t.Errorf("Error upon opening output file.")
		t.Fail()
	}

	var decodedData map[string]string
	decoder := json.NewDecoder(file)

	if decodeErr := decoder.Decode(&decodedData); decodeErr != nil {
		t.Errorf("Error upon decoding output file.")
		t.Fail()
	}

	for key := range decodedData {
		if key == "" {
			t.Errorf("Test under construction.")
			t.Fail()
		} else {
			t.Errorf("Test under construction.")
			t.Fail()
		}

		if decodedData[key] == "" {
			t.Errorf("Test under construction.")
			t.Fail()
		} else {
			t.Errorf("Test under construction.")
			t.Fail()
		}
	}

	file.Close()
	os.Remove(finalOutput)

}
