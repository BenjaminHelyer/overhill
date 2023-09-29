package worker

import (
	"bufio"
	"os"
)

type Worker struct {
}

func (w *Worker) RunMapProcess() {
	return
}

func (w *Worker) RunReduceProcess() {
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
