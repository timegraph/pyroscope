package agent

import (
	"time"

	"github.com/pyroscope-io/pyroscope/pkg/agent/types"
	"github.com/pyroscope-io/pyroscope/pkg/agent/upstream"
	"github.com/pyroscope-io/pyroscope/pkg/config"
	"github.com/pyroscope-io/pyroscope/pkg/util/atexit"
	"github.com/sirupsen/logrus"
)

func SelfProfile(cfg *config.Config, u upstream.Upstream, appName string, logger Logger) error {
	sc := SessionConfig{
		Upstream:         u,
		AppName:          appName,
		ProfilingTypes:   types.DefaultProfileTypes,
		SpyName:          types.GoSpy,
		SampleRate:       uint32(cfg.Server.SampleRate),
		UploadRate:       10 * time.Second,
		Pid:              0,
		WithSubprocesses: false,
	}
	session := NewSession(&sc, logrus.New())
	if err := session.Start(); err != nil {
		return err
	}

	atexit.Register(session.Stop)
	return nil
}
