package worker

func ReadFromFile(filepath string) string {
	return ""
}

func WriteToFile(filepath string, contents string) {
	return
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
