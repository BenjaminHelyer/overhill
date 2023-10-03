package coordinator

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var mockWorkerLazyReliable *httptest.Server

func TestMain(m *testing.M) {
	// this mock doesn't check params, but it always sends back a response indicating the job is done
	mockWorkerLazyReliable = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		responseBody := "Complete"
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(responseBody))
	}))
	defer mockWorkerLazyReliable.Close()

	exitCode := m.Run()
	os.Exit(exitCode)
}

// Coordinator shall send a HTTP request to workers to run a Map function
// the state of the MapReduce job shall be updated upon receiving a confirmation
// from the worker
func TestSendMapRequest(t *testing.T) {
	workerUrl := mockWorkerLazyReliable.URL
	expectedResponse := "Complete"
	response, mapError := SendMapRequest(workerUrl, "wc_total", "../storage/..", "intermediate.json")
	if mapError != nil {
		t.Errorf("Error raised after sending Map request: %v", mapError)
	}
	if response != expectedResponse {
		t.Errorf("Response to Map request was %v, expected response was %v", response, expectedResponse)
	}
}

// Coordinator shall send a HTTP request to workers to run a Reduce function
// the state of the MapReduce job shall be updated upon receiving a confirmation
// from the worker
func TestSendReduceRequest(t *testing.T) {
	workerUrl := mockWorkerLazyReliable.URL
	expectedResponse := "Complete"
	response, mapError := SendReduceRequest(workerUrl, "wc_total", "../storage/..", "intermediate.json")
	if mapError != nil {
		t.Errorf("Error raised after sending Reduce request: %v", mapError)
	}
	if response != expectedResponse {
		t.Errorf("Response to Reduce request was %v, expected response was %v", response, expectedResponse)
	}
}

// Coordinator shall load list of known worker servers
// from a config file
func TestLoadConfig(t *testing.T) {
	configFilepath := "test_resources/mocked_workers.json"
	// initialize expected urls to false to symbolize
	// that they are not yet found
	expectedWorkerUrls := map[string]bool{
		"my_worker":     false,
		"second_worker": false,
	}

	var uutCoordinator Coordinator
	loadErr := uutCoordinator.LoadConfig(configFilepath)

	if loadErr != nil {
		t.Errorf("Error upon loading from config file: %v", loadErr)
		t.Fail()
	}

	for workerUrl := range uutCoordinator.workerStatus {
		if _, exists := expectedWorkerUrls[workerUrl]; exists {
			expectedWorkerUrls[workerUrl] = true
		} else {
			t.Errorf("Did not correctly load all URLs from config file. Found unexpected URL: %v", workerUrl)
			t.Fail()
		}
	}

	for url := range expectedWorkerUrls {
		if expectedWorkerUrls[url] == false {
			t.Errorf("Did not correctly load all URLs from config file. This URL was not loaded: %v", url)
			t.Fail()
		}
	}
}

func TestPartitionFolder(t *testing.T) {
	folderName := "test_resources/example_folder"
	// initialize expected partitions to false to symbolize
	// that they are not yet found
	expectedPartitions := map[string]bool{
		"t1.txt": false,
		"t2.txt": false,
		"t3.txt": false,
	}

	var uutCoordinator Coordinator
	uutCoordinator.PartitionFolder(folderName)

	if len(uutCoordinator.mapPartitionStatus) != len(expectedPartitions) {
		t.Errorf("Expected %v number of partitions, but got %v number.", expectedPartitions, len(uutCoordinator.mapPartitionStatus))
		t.Fail()
	}

	for partition := range uutCoordinator.mapPartitionStatus {
		if _, exists := expectedPartitions[partition]; exists {
			expectedPartitions[partition] = true
		} else {
			t.Errorf("Did not correctly load all partitions input folder. Found unexpected partition: %v", partition)
			t.Fail()
		}
	}

	for partition := range expectedPartitions {
		if expectedPartitions[partition] == false {
			t.Errorf("Did not correctly load all parititions from input folder. This partition was not loaded: %v", partition)
			t.Fail()
		}
	}
}
