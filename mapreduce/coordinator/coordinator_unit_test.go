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

// The state of the coordinator shall be updated upon
// the completion of a Map request
func TestUpdateStateMapCompletion(t *testing.T) {

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

// The state of the coordinator shall be updated upon
// the completion of a Reduce request
func TestUpdateStateReduceCompletion(t *testing.T) {

}

// Coordinator shall load list of known worker servers
// from a config file
func TestLoadConfig(t *testing.T) {

}

// Coordinator shall send multiple map requests in parallel,
// waiting for confirmations on all of them (does not test faults for now)
func TestParallelMapRequests(t *testing.T) {

}

// Coordinator shall send multiple reduce requests in parallel,
// waiting for confirmations on all of them (does not test faults for now)
func TestParallelReduceRequests(t *testing.T) {

}
