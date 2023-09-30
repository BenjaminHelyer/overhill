package worker

import (
	"bufio"
	"encoding/json"
	"os"
	"strconv"
	"strings"
)

type KeyValue struct {
	Key   string
	Value string
}

type WorkerFuncs interface {
	EmitIntermediate()
	EmitFinal()
}

type Worker struct {
	// Assumption: intermediate values are small
	// enough for each partition that they can be held in-memory
	emitttedIntermediates []KeyValue
	emittedFinals         []KeyValue
}

func (w *Worker) RunMapProcess(filepath string, mapFuncKey string) {
	// Step 1: Get inputs for Map function
	// 1A: Read from file
	// Assumption: inputs are in the Map workers' local disks
	// this assumption comes from the same way Google ran
	// both GFS and MapReduce on the same set of machines
	// Assumption: For now we'll stick with one file per Map process
	inputFileContents, fileErr := ReadFromFile(filepath) // TODO: make this function part of worker struct
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
	processedInputKeys, processedInputValues := PreProcessFileInput(mapFuncKey, filepath, inputFileContents)

	// Step 2: Run map function on file inputs
	for index, key := range processedInputKeys {
		value := processedInputValues[index]
		userSpecifiedFunc(key, value, w.EmitIntermediate)
		// call emit inside the Map function
	}

	// Step 3: Write outputs to local disk
	// read from emitted values, which will be stored in the Worker struct
	WriteToJson("intermediate.json", w.emitttedIntermediates)

	return
}

// TODO: likely need to change this later such that it works on a given set of intermediate keys
// i.e., rather than handing it a single filepath, hand it the key to search for along with the addresses of all machines
// which have run a Map function
func (w *Worker) RunReduceProcess(intermediateJsonpath string, reduceFuncKey string) {
	// Step 1: Read remotely from another worker's disk
	inputJsonContents, jsonErr := ReadFromJson(intermediateJsonpath)
	if jsonErr != nil {
		// TODO: do something upon jsonErr
	}

	userSpecifiedFunc := ProduceReduceFunction(reduceFuncKey)

	// Step 2: Combine all values for a given key
	// Assume for now that all values for a given intermediate
	// key can be held in memory.

	// TODO: have this accept multiple keys and likely have a function inside that
	// loops over all the keys handed to this one, combining each one
	// For now we just hand it a json with key-value pairs and have it
	// combine everything inside that JSON
	combinedKvs := CombineValuesForKeys(inputJsonContents)

	// Step 3: Run reduce function on inputs held in memory
	for key := range combinedKvs {
		userSpecifiedFunc(key, combinedKvs[key], w.EmitFinal)
	}

	// Step 4: Write outputs to file system
	// Locally, this can just be a separate folder
	// Remotely, we'd want an actual file system somehow
	WriteToJson("final.json", w.emittedFinals)

	return
}

func ProduceMapFunction(mapFuncKey string) MapFunc {
	// just return word count example for now
	return mapWordCount
}

func ProduceReduceFunction(reduceFuncKey string) ReduceFunc {
	// just return word count example for now
	return reduceWordCount
}

func PreProcessFileInput(mapFuncKey string, filepath string, inputFileContents string) ([]string, []string) {
	// just return identity for now
	return []string{filepath}, []string{inputFileContents}
}

func CombineValuesForKeys(kvs []KeyValue) map[string][]string {
	kvMap := make(map[string][]string)
	for _, kv := range kvs {
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}
	return kvMap
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

func WriteToJson(jsonpath string, kvPairs []KeyValue) error {
	file, fileCreationError := os.Create(jsonpath)
	if fileCreationError != nil {
		return fileCreationError
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	if encodingError := encoder.Encode(kvPairs); encodingError != nil {
		return encodingError
	}

	return nil
}

func ReadFromJson(jsonpath string) ([]KeyValue, error) {
	file, fileOpenError := os.Open(jsonpath)
	if fileOpenError != nil {
		// TODO: do something on a file open error
	}

	var decodedData []KeyValue
	decoder := json.NewDecoder(file)

	if decodeErr := decoder.Decode(&decodedData); decodeErr != nil {
		// TODO: do something on a decode error
	}

	return decodedData, nil
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

func (w *Worker) EmitIntermediate(intermediateKey string, intermediateValue string) {
	kv := KeyValue{
		Key:   intermediateKey,
		Value: intermediateValue,
	}
	w.emitttedIntermediates = append(w.emitttedIntermediates, kv)
}

func RunMapFunc(userFunc MapFunc, inputKey string, inputVal string) (string, string) {
	return "", ""
}

func (w *Worker) EmitFinal(outputKey string, outputVals []string) {
	kv := KeyValue{
		Key:   outputKey,
		Value: strings.Join(outputVals, ","),
	}
	w.emittedFinals = append(w.emittedFinals, kv)
}

func RunReduceFunc(userFunc ReduceFunc, inputKey string, inputVals []string) (string, []string) {
	return "", []string{"", ""}
}

/*
* ----- Begin built-in Map functions -----
 */

// pass emit to the user function so that they can easily mock it
// separates concerns from MapFunc and EmitIntermediate
type MapFunc func(inputKey string, inputVal string, emit func(string, string))
type ReduceFunc func(inputKey string, inputVals []string, emit func(string, []string))

func mapWordCount(filename string, contents string, emit func(string, string)) {
	words := strings.Fields(contents)
	for _, word := range words {
		emit(word, "1")
	}
}

func reduceWordCount(word string, counts []string, emit func(string, []string)) {
	// since we assume all word count values are one, just return length of value slice
	emit(word, []string{strconv.Itoa(len(counts))})
}
