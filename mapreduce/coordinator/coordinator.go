package coordinator

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
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

const INTERMEDIATE_FOLDER = "intermediate/"

func (c *Coordinator) RunCoordinator(configFilepath string, mapFunc string, reduceFunc string, inputFolder string) (string, error) {
	// Step 1: Load the config
	configErr := c.LoadConfig(configFilepath)
	if configErr != nil {
		return "", configErr
	} else if len(c.workerStatus) == 0 {
		return "", fmt.Errorf("Did not find any workers in config file.")
	}

	// Step 2: Partition the input folder contents (just by individual files for now)
	// Note that later, input folder could be on a filesystem or object store rather than locally
	paritionError := c.PartitionFolder(inputFolder, "map")
	if paritionError != nil {
		return "", paritionError
	}

	// Step 3: Assign map tasks to workers in the config
	// (3a) Start off different threads for each worker
	// (3b) Periodically check on each worker until completion
	// (3c) Once completed, update the status of each worker as well as the Map partition status
	mapError := c.RunMapWorkers(mapFunc, inputFolder, INTERMEDIATE_FOLDER)

	if mapError != nil {
		return "", mapError
	}

	// Step 4: Partition the intermediate files (just by indepedent worker outputs for now)
	partitionError := c.PartitionFolder(INTERMEDIATE_FOLDER, "reduce")
	if partitionError != nil {
		return "", partitionError
	}

	// Step 5: Assign reduce tasks to workers
	// (5a) Start off different threads for each worker
	// (5b) Periodically check on each worker until completion
	// (5c) Once completed, update the status of each worker as well as the Reduce partition status
	reduceError := c.RunReduceWorkers(reduceFunc, INTERMEDIATE_FOLDER)

	if reduceError != nil {
		return "", reduceError
	}

	// Step 6: Return the filepath to the final output as well as any errors
	return "", nil
}

func (c *Coordinator) PartitionFolder(folderPath string, taskType string) error {
	dir, openError := os.Open(folderPath)
	if openError != nil {
		return openError
	}
	defer dir.Close()

	entries, contentsError := dir.ReadDir(-1)
	if contentsError != nil {
		return contentsError
	}

	if taskType == "map" {
		for _, entry := range entries {
			c.mapPartitionStatus[string(entry.Name())] = "unprocessed"
		}
	} else if taskType == "reduce" {
		for _, entry := range entries {
			c.reducePartitionStatus[string(entry.Name())] = "unprocessed"
		}
	} else {
		return fmt.Errorf("Received unexpected task type %v, task type must be 'map' or 'reduce' exactly.", taskType)
	}

	return nil
}

func (c *Coordinator) RunMapWorkers(mapFunc string, inputFolder string, intermediateFolder string) error {
	// desired behavior:
	// assign as many partition tasks as we can at the beginning
	// check on all assigned workers every X seconds (where X is a design parameter)
	// if assigned worker has completed, mark task as done
	// hand it another task if there are remaining tasks
	// if assigned worker hasn't completed, wait another Y iterations (where Y is a design parameter),
	// then if it still hasn't completed, mark the worker as failed and assign the task to another worker

	// error variable to return
	var mapError error
	mapError = nil

	// initialize stack of partitions needing a worker with all partitions
	var partitionsNeedWorker []string
	for partition := range c.mapPartitionStatus {
		partitionsNeedWorker = append(partitionsNeedWorker, partition)
	}

	// initialize stack of idle workers needing a task
	var idleWorkers []string
	for worker := range c.workerStatus {
		idleWorkers = append(idleWorkers, worker)
	}

	// TODO: implement a thread to assign incomplete tasks to idle workers
	// there should also be one thread for each assigned task

	// thread to check on all partitions
	// the process will wait on this thread to finish before
	// either exiting or moving onto the reduce step
	var mapWait sync.WaitGroup
	allDone := false
	iteration := 0
	go func(partitionsNeedWorker []string) {
		for allDone != true || iteration < 10 {
			time.Sleep(1) // TODO: change this into a design parameter somewhere
			allDone = true
			for _, status := range c.mapPartitionStatus {
				// in the go task for each worker request, we'll mark the task as complete
				if status != "complete" {
					allDone = false
					// TODO: determine if this worker if failed.
					// if it has, mark the partition status as incomplete and put it back on the stack
				}
			}
			iteration++
		}
		// TODO: update mapError on non-recoverable failures
		// only move onto Reduce when the above loop terminates
		defer mapWait.Done()
	}(partitionsNeedWorker)

	var firstPartition string
	for partition := range c.mapPartitionStatus {
		firstPartition = partition
		break
	}

	// TODO: handle faults if a particular Map task doesn't complete
	for workerUrl := range c.workerStatus {
		response, workerErr := SendMapRequest(workerUrl, mapFunc, inputFolder+firstPartition, intermediateFolder+"test"+"_intermediate.json")

		// TODO: rather than raising here, mark the worker as bad and assign the map task to another worker
		// but later need to consider the case where map task itself is bad
		if response != "Complete" {
			return fmt.Errorf("Received bad response from worker: %v", response)
		} else if workerErr != nil {
			return workerErr
		}
	}

	mapWait.Wait()
	return mapError
}

func (c *Coordinator) RunReduceWorkers(reduceFunc string, intermediateFolder string) error {
	var firstPartition string
	for partition := range c.reducePartitionStatus {
		firstPartition = partition
		break
	}

	for workerUrl := range c.workerStatus {
		// TODO: rather than combining here, let's store the path to the partition in the partition map
		response, workerErr := SendReduceRequest(workerUrl, reduceFunc, intermediateFolder+firstPartition, "test"+"_final.json")

		// TODO: rather than raising here, mark the worker as bad and assign the map task to another worker
		// but later need to consider the case where map task itself is bad
		if response != "Complete" {
			return fmt.Errorf("Received bad response from worker: %v", response)
		} else if workerErr != nil {
			return workerErr
		}
	}
	return nil
}

func (c *Coordinator) LoadConfig(configFilepath string) error {
	// n.b. we expect (for now) that the config file will be a .json
	file, fileOpenError := os.Open(configFilepath)
	if fileOpenError != nil {
		return fileOpenError
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if decodeErr := decoder.Decode(&c.workerStatus); decodeErr != nil {
		return decodeErr
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
