package framework

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
)

type CommonTask struct {
	*TaskData
}

func (ct *CommonTask) Data() *TaskData {
	return ct.TaskData
}

func (ct *CommonTask) commonMatches(offer *mesos.Offer) string {
	if ct.Cpu > getScalarResources(offer, "cpus") {
		return "no cpus"
	}

	if ct.Mem > getScalarResources(offer, "mem") {
		return "no mem"
	}

	return ""
}
