package stockpile

import (
	"fmt"
	"regexp"
	"time"

	log "github.com/cihub/seelog"
)

var Logger log.LoggerInterface

var Config *config = &config{
	FrameworkName:    "go_kafka_client",
	FrameworkRole:    "*",
	FrameworkTimeout: 30 * time.Minute,
	LogLevel:         "info",
	Storage:          "file:go_kafka_client_mesos.json",
}

var executorMask = regexp.MustCompile("executor.*")

type config struct {
	Api              string
	Master           string
	FrameworkName    string
	FrameworkRole    string
	FrameworkTimeout time.Duration
	User             string
	Executor         string
	LogLevel         string
	Storage          string
}

func (c *config) String() string {
	return fmt.Sprintf(`api:                 %s
master:              %s
framework name:      %s
framework role:      %s
framework timeout    %s
user:                %s
executor:            %s
log level:           %s
storage:             %s
`, c.Api, c.Master, c.FrameworkName, c.FrameworkRole, c.FrameworkTimeout, c.User, c.Executor, c.LogLevel, c.Storage)
}

func InitLogging(level string) error {
	config := fmt.Sprintf(`<seelog minlevel="%s">
    <outputs formatid="main">
        <console />
    </outputs>

    <formats>
        <format id="main" format="%%Date/%%Time [%%LEVEL] %%Msg%%n"/>
    </formats>
</seelog>`, level)

	logger, err := log.LoggerFromConfigAsBytes([]byte(config))
	Config.LogLevel = level
	Logger = logger

	return err
}
