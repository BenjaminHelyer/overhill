package main

import "flag"

func main() {
	var localPort string
	var isCoordinator bool
	var coordConfig string
	var coordMapFunc string
	var coordInput string
	var coordReduceFunc string

	flag.StringVar(&localPort, "port", "5000", "Local Port")
	flag.BoolVar(&isCoordinator, "coord", false, "Make this instance a coordinator")
	flag.StringVar(&coordConfig, "config", "config.json", "Path to the configuration file for the coordinator")
	flag.StringVar(&coordMapFunc, "mapFunc", "wc_total", "Specification of the map function")
	flag.StringVar(&coordReduceFunc, "reduceFunc", "wc_total", "Specification of the reduce function")
	flag.StringVar(&coordInput, "input", "", "Path to input folder")
	flag.Parse()

	if isCoordinator {
		print("Starting up Coordinator process...\n")
		coordErr := BootupCoordinator(coordConfig, coordMapFunc, coordReduceFunc, coordInput)
		if coordErr != nil {
			print("Received error from coordinator: ", coordErr.Error(), "\n")
		} else {
			print("Coordinator terminated successfully.\n")
		}
	} else {
		print("Starting up Worker server on port ", localPort, "...\n")
		BootupWorker(localPort)
	}
}
