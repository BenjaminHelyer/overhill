package worker

import (
	"bufio"
	"os"
)

type Worker struct {
}

func (w *Worker) RunMapProcess(filepath string, mapFuncKey string) {
	// Step 1: Get inputs for Map function
	// 1A: Read from file
	// Assumption: inputs are in the Map workers' local disks
	// this assumption comes from the same way Google ran
	// both GFS and MapReduce on the same set of machines
	inputFile, fileErr := ReadFromFile(filepath) // TODO: make this function part of worker struct
	if fileErr != nil {
		// TODO: do something upon an error
	}

	// 1B: determine given Map function
	// Assumption: map functions can be lookedup by keys,
	// i.e., user just provides a pre-specified key
	// that runs a pre-programmed function on the cluster
	userSpecifiedFunc := ProduceMapFunction(mapFuncKey) // factory function to produce an instance of the desired function

	// 1C: pre-process string from file to hand to Map function
	// this may just be an identity function for some values of mapFuncKey
	// helps us to hand the map function input of a format that it expects
	processedInputKeys, processedInputValues := PreProcessFileInput(mapFuncKey, inputFile)

	// Step 2: Run map function on file inputs
	for index, key := range processedInputKeys {
		value := processedInputValues[index]
		userSpecifiedFunc(key, value, EmitIntermediate)
		// call emit inside the Map function
	}

	// Step 3: Write outputs to local disk
	// read from emitted values, which will be stored in the Worker struct
	return
}

func ProduceMapFunction(mapFuncKey string) MapFunc {
	return nil
}

func PreProcessFileInput(mapFuncKey string, inputFileContents string) ([]string, []string) {
	return []string{""}, []string{""}
}

func (w *Worker) RunReduceProcess() {
	// Step 1: Read remotely from another worker's disk

	// Step 2: Run reduce function on inputs held in memory
	// Cannot assume that all values for a given intermediate key
	// can be held in memory.

	// Step 3: Write outputs to file system
	// Locally, this can just be a separate folder
	// Remotely, we'd want an actual file system somehow

	return
}

func ReadFromFile(filepath string) (string, error) {
	file, fileOpenError := os.Open(filepath)
	if fileOpenError != nil {
		return "", fileOpenError
	}
	defer file.Close()

	fileText := ""

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		currLine := scanner.Text()
		fileText += currLine
	}

	// interestingly, go lets us separate initialization
	// from condition via the ;
	if scannerError := scanner.Err(); scannerError != nil {
		return "", scannerError
	}

	return fileText, nil
}

func WriteToFile(filepath string, contents string) error {
	file, fileCreationError := os.Create(filepath)
	if fileCreationError != nil {
		return fileCreationError
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	_, writeError := writer.WriteString(contents)
	if writeError != nil {
		return writeError
	}

	// ensure all data is written to file before closing the file
	flushError := writer.Flush()
	if flushError != nil {
		return flushError
	}

	return nil
}

// pass emit to the user function so that they can easily mock it
// separates concerns from MapFunc and EmitIntermediate
type MapFunc func(inputKey string, inputVal string, emit func(string, string))

func EmitIntermediate(intermediateKey string, intermediateValue string) {
	return
}

func RunMapFunc(userFunc MapFunc, inputKey string, inputVal string) (string, string) {
	return "", ""
}

// TODO: make the second argument an iterator rather than an array
type ReduceFunc func(inputKey string, inputVals []string, emit func(string, []string))

func EmitFinal(outputKey string, outputVals []string) {

}

func RunReduceFunc(userFunc ReduceFunc, inputKey string, inputVals []string) (string, []string) {
	return "", []string{"", ""}
}
