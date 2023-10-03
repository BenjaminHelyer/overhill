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
