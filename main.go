package main

import (
	"flag"
	"os"

	stockpile "github.com/elodina/stockpile/executor"
	"github.com/mesos/mesos-go/executor"
)

var logLevel = flag.String("log.level", "info", "Log level. trace|debug|info|warn|error|critical. Defaults to info.")

func main() {
	flag.Parse()
	stockpile.InitLogging(*logLevel)
	taskExecutor := stockpile.NewExecutor()

	driverConfig := executor.DriverConfig{
		Executor: taskExecutor,
	}

	driver, err := executor.NewMesosExecutorDriver(driverConfig)
	if err != nil {
		stockpile.Logger.Error(err)
		os.Exit(1)
	}

	_, err = driver.Start()
	if err != nil {
		stockpile.Logger.Error(err)
		os.Exit(1)
	}
	driver.Join()
}
