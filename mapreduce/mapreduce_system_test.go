package main

import (
	"main/coordinator"
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

}
