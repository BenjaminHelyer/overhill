package main

import "main/coordinator"

func BootupCoordinator(configFilepath string, mapFunc string, reduceFunc string, inputFolder string) error {
	coord := coordinator.NewCoordinator()
	_, coordError := coord.RunCoordinator(configFilepath, mapFunc, reduceFunc, inputFolder)
	return coordError
}
