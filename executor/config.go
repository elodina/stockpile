package stockpile

import (
	"fmt"

	log "github.com/cihub/seelog"
)

var Logger log.LoggerInterface

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
