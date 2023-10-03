package coordinator

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type Coordinator struct {
	configFilepath        string
	mapPartitionStatus    map[string]string
	reducePartitionStatus map[string]string
	workerStatus          map[string]string
}

func NewCoordinator() *Coordinator {
	return &Coordinator{
		configFilepath:        "",
		mapPartitionStatus:    make(map[string]string),
		reducePartitionStatus: make(map[string]string),
		workerStatus:          make(map[string]string),
	}
}

func (c *Coordinator) RunCoordinator(configFilepath string, mapFunc string, inputFolder string) (string, error) {
	// Step 1: Load the config
	c.LoadConfig(configFilepath)

	// Step 2: Partition the input folder contents (just by individual files for now)
	// Note that later, input folder could be on a filesystem or object store rather than locally
	c.PartitionFolder(inputFolder)

	// Step 3: Assign map tasks to workers in the config
	// (3a) Start off different threads for each worker
	// (3b) Periodically check on each worker until completion
	// (3c) Once completed, update the status of each worker as well as the Map partition status

	// Step 4: Partition the intermediate files (just by indepedent worker outputs for now)

	// Step 5: Assign reduce tasks to workers
	// (5a) Start off different threads for each worker
	// (5b) Periodically check on each worker until completion
	// (5c) Once completed, update the status of each worker as well as the Reduce partition status

	// Step 6: Return the filepath to the final output as well as any errors
	return "", nil
}

func (c *Coordinator) PartitionFolder(folderPath string) error {
	dir, openError := os.Open(folderPath)
	if openError != nil {
		return openError
	}
	defer dir.Close()

	entries, contentsError := dir.ReadDir(-1)
	if contentsError != nil {
		return contentsError
	}

	for _, entry := range entries {
		c.mapPartitionStatus[string(entry.Name())] = "unprocessed"
	}

	return nil
}

func (c *Coordinator) LoadConfig(configFilepath string) error {
	// n.b. we expect (for now) that the config file will be a .json
	file, fileOpenError := os.Open(configFilepath)
	if fileOpenError != nil {
		// TODO: do something on a file open error
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if decodeErr := decoder.Decode(&c.workerStatus); decodeErr != nil {
		// TODO: do something on a decode error
	}

	return nil
}

func SendMapRequest(url string, mapFunc string, inputFilepath string, intermediateFilepath string) (string, error) {
	parametrizedUrl := url + fmt.Sprintf("/map?func=%v&input=%v&output=%v", mapFunc, inputFilepath, intermediateFilepath)
	response, requestErr := http.Get(parametrizedUrl)
	if requestErr != nil {
		return "", requestErr
	}
	defer response.Body.Close()

	body, bodyErr := ReadAndRaiseResponse(*response)

	return body, bodyErr
}

func SendReduceRequest(url string, mapFunc string, intermediateFilepath string, finalFilepath string) (string, error) {
	parametrizedUrl := url + fmt.Sprintf("/reduce?func=%v&input=%v&output=%v", mapFunc, intermediateFilepath, finalFilepath)
	response, requestErr := http.Get(parametrizedUrl)
	if requestErr != nil {
		return "", requestErr
	}
	defer response.Body.Close()

	body, bodyErr := ReadAndRaiseResponse(*response)

	return body, bodyErr
}

func ReadAndRaiseResponse(response http.Response) (string, error) {
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP Get request failed with status code: %v", response.StatusCode)
	}

	body, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		return "", readErr
	}

	return string(body), nil
}
