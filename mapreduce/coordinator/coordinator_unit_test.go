package coordinator

import "testing"

// Coordinator shall send a HTTP request to workers to run a Map function
// the state of the MapReduce job shall be updated upon receiving a confirmation
// from the worker
func TestSendMapRequest(t *testing.T) {
	expectedResponse := "Complete"
	response, mapError := SendMapRequest("url", "wc_total", "../storage/..", "intermediate.json")
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
	expectedResponse := "Complete"
	response, mapError := SendReduceRequest("url", "wc_total", "../storage/..", "intermediate.json")
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
