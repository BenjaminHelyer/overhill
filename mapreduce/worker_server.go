package main

import (
	"fmt"
	"io"
	"main/worker"
	"net/http"
)

func getHello(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Received a hello request\n")
	io.WriteString(w, "Hello, World!")
}

func getRunMap(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Received a request to run a Map task\n")

	mapFuncName := r.URL.Query().Get("func")
	fmt.Printf("Function name requested was %v\n", mapFuncName)

	inputFilename := r.URL.Query().Get("input")
	fmt.Printf("File requested to query was %v\n", inputFilename)

	outputFilename := r.URL.Query().Get("output")
	fmt.Printf("Output file requested was %v\n", outputFilename)

	fmt.Printf("Running map task...\n ")
	var task worker.Worker
	task.RunMapProcess(inputFilename, mapFuncName, outputFilename)

	fmt.Printf("Finished map task. Intermediate output can be found at %v \n", outputFilename)
	io.WriteString(w, "Complete")
}

func getRunReduce(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("Received a request to run a Reduce task\n")

	mapFuncName := r.URL.Query().Get("func")
	fmt.Printf("Function name requested was %v\n", mapFuncName)

	intermediateFilename := r.URL.Query().Get("input")
	fmt.Printf("File requested to query was %v\n", intermediateFilename)

	outputFilename := r.URL.Query().Get("output")
	fmt.Printf("Output file requested was %v\n", outputFilename)

	fmt.Printf("Running reduce task...\n")
	var task worker.Worker
	task.RunReduceProcess(intermediateFilename, mapFuncName, outputFilename)

	fmt.Printf("Finished reduce task. Final output can be found at %v \n", outputFilename)
	io.WriteString(w, "Complete")
}

func BootupWorker(port string) {
	http.HandleFunc("/hello", getHello)
	http.HandleFunc("/map", getRunMap)
	http.HandleFunc("/reduce", getRunReduce)

	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		fmt.Printf(err.Error())
	}
}
