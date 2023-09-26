package worker

func readFromFile(filepath string) string {
	return ""
}

func writeToFile(filepath string) {
	return
}

type MapFunc func(string, string) (string, string)

func runMapFunc(userFunc MapFunc, input_key string, input_val string) (string, string) {
	return "", ""
}

// TODO: make the second argument an iterator rather than an array
type ReduceFunc func(string, [10]string) (string, [2]string)

func runReduceFunc(userFunc ReduceFunc, input_key string, input_vals [10]string) (string, [2]string) {
	return "", [2]string{"", ""}
}
