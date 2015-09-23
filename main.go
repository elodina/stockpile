package main

import (
	"flag"
	"fmt"
	"os"

	stockpile "github.com/elodina/stockpile/executor"
	"github.com/mesos/mesos-go/executor"
)

var logLevel = flag.String("log.level", "debug", "Log level. trace|debug|info|warn|error|critical. Defaults to info.")
var taskType = flag.String("type", "", "Task type.")

func main() {
	fmt.Println("Starting main")
	flag.Parse()
	err := stockpile.InitLogging(*logLevel)
	if err != nil {
		fmt.Printf("Logging init failed: %s", err.Error())
	}
	stockpile.Logger.Debug("Creating new executor")
	taskExecutor := stockpile.NewExecutor()
	stockpile.Logger.Debug("Executor created")

	driverConfig := executor.DriverConfig{
		Executor: taskExecutor,
	}

	driver, err := executor.NewMesosExecutorDriver(driverConfig)
	if err != nil {
		stockpile.Logger.Error(err)
		os.Exit(1)
	}

	stockpile.Logger.Debug("Starting driver")
	_, err = driver.Start()
	if err != nil {
		stockpile.Logger.Error(err)
		os.Exit(1)
	}
	driver.Join()
}
