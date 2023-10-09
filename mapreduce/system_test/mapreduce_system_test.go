package systemTest

import (
	"os"
	"os/exec"
	"sync"
	"testing"
)

// we shall get the total word count of
// several works of Emerson via MapReduce;
// the works are kept in the storage folder
// and distributed among the workers for
// the Map functions.
// For now only one worker will run a Reduce
// function (we don't worry about partitioning
// for Reduce right now)
func TestEmersonWordCount_SingleWorker(t *testing.T) {
	coordConfigFile := "../coordinator/test_resources/single_mocked_worker.json"
	emersonFolder := "test_storage"

	var wg sync.WaitGroup

	workerArgs := []string{"--port=5050"}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runSubprocess("../main.exe", workerArgs); err != nil {
			t.Errorf("Error running worker subprocess: %v", err)
			t.Fail()
		}
	}()

	coordinatorArgs := []string{"--coord", "--config=" + coordConfigFile, "--mapFunc=wc_total", "--reduceFunc=wc_total", "--input=" + emersonFolder}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runSubprocess("../main.exe", coordinatorArgs); err != nil {
			t.Errorf("Error running coordinator subprocess: %v", err)
			t.Fail()
		}
	}()

	wg.Wait()

	// _, err := os.Stat(filepath)
	// if err != nil {
	// 	t.Errorf("Output file does not exist: %v", err)
	// 	t.Fail()
	// }

	// file, fileOpenError := os.Open(filepath)
	// if fileOpenError != nil {
	// 	t.Errorf("Error upon opening output file.")
	// 	t.Fail()
	// }

	// fileText := ""

	// scanner := bufio.NewScanner(file)
	// for scanner.Scan() {
	// 	currLine := scanner.Text()
	// 	fileText += currLine
	// }

	// if fileText != contents {
	// 	t.Errorf("Read text does not match input contents. Input contents were: %v", contents)
	// 	t.Fail()
	// }

	// file.Close()
	// os.Remove(filepath)
	// if err != nil {
	// 	t.Errorf("Error deleting")
	// }
}

func runSubprocess(name string, args []string) error {
	print("***\n")
	print("Running subprocess: ", name, " with args: ", args, "\n")
	print("***\n")
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
