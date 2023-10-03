package coordinator

type Coordinator struct {
	configFilepath string
	taskStatus     map[string]string
	workerStatus   map[string]string
}

func LoadConfig(coord Coordinator, configFilepath string) error {
	return nil
}

func SendMapRequest(url string, mapFunc string, inputFilepath string, intermediateFilepath string) (string, error) {
	return "", nil
}

func SendReduceRequest(url string, mapFunc string, intermediateFilepath string, finalFilepath string) (string, error) {
	return "", nil
}
