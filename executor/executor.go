package stockpile

import (
	"encoding/json"
	"os"

	"github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	kafkamesos "github.com/stealthly/go_kafka_client/mesos/framework"
)

type Executor struct {
	config            kafkamesos.TaskConfig
	kafkaConsumer     *KafkaConsumer
	cassandraProducer *CassandraProducer
}

func NewExecutor() *Executor {
	executor := &Executor{
		config:            make(kafkamesos.TaskConfig),
		kafkaConsumer:     NewKafkaConsumer(),
		cassandraProducer: NewCassandraProducer(),
	}
	return executor
}

func (e *Executor) start() {
	e.cassandraProducer.start()
	e.kafkaConsumer.start()
}

func (e *Executor) stop() {
	e.kafkaConsumer.stop()
	e.cassandraProducer.stop()
}

func (e *Executor) Registered(driver executor.ExecutorDriver, executor *mesos.ExecutorInfo, framework *mesos.FrameworkInfo, slave *mesos.SlaveInfo) {
	Logger.Infof("[Registered] framework: %s slave: %s", framework.GetId().GetValue(), slave.GetId().GetValue())
}

func (e *Executor) Reregistered(driver executor.ExecutorDriver, slave *mesos.SlaveInfo) {
	Logger.Infof("[Reregistered] slave: %s", slave.GetId().GetValue())
}

func (e *Executor) Disconnected(executor.ExecutorDriver) {
	Logger.Info("[Disconnected]")
}

func (e *Executor) LaunchTask(driver executor.ExecutorDriver, task *mesos.TaskInfo) {
	Logger.Infof("[LaunchTask] %s", task)

	err := json.Unmarshal(task.GetData(), &e.config)
	if err != nil {
		Logger.Errorf("Could not unmarshal json data: %s", err)
		panic(err)
	}

	Logger.Info(e.config)

	runStatus := &mesos.TaskStatus{
		TaskId: task.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}

	if _, err := driver.SendStatusUpdate(runStatus); err != nil {
		Logger.Errorf("Failed to send status update: %s", runStatus)
		os.Exit(1) //TODO not sure if we should exit in this case, but probably yes
	}

	go func() {
		e.start()

		// finish task
		Logger.Infof("Finishing task %s", task.GetName())
		finStatus := &mesos.TaskStatus{
			TaskId: task.GetTaskId(),
			State:  mesos.TaskState_TASK_FINISHED.Enum(),
		}
		if _, err := driver.SendStatusUpdate(finStatus); err != nil {
			Logger.Errorf("Failed to send status update: %s", finStatus)
			os.Exit(1)
		}
		Logger.Infof("Task %s has finished", task.GetName())
	}()
}

func (e *Executor) KillTask(driver executor.ExecutorDriver, id *mesos.TaskID) {
	Logger.Infof("[KillTask] %s", id.GetValue())
	e.stop()
}

func (e *Executor) FrameworkMessage(driver executor.ExecutorDriver, message string) {
	Logger.Infof("[FrameworkMessage] %s", message)
}

func (e *Executor) Shutdown(driver executor.ExecutorDriver) {
	Logger.Infof("[Shutdown]")
	e.stop()
}

func (e *Executor) Error(driver executor.ExecutorDriver, message string) {
	Logger.Errorf("[Error] %s", message)
}
