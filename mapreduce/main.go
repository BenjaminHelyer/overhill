package main

func main() {
	isCoordinator := false
	if isCoordinator {
		BootupCoordinator("", "", "")
	} else {
		BootupWorker()
	}
}
