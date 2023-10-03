package main

import "main/coordinator"

func BootupCoordinator(configFilepath string, mapFunc string, inputFolder string) {
	var coord coordinator.Coordinator
	coord.RunCoordinator(configFilepath, mapFunc, inputFolder)
}
