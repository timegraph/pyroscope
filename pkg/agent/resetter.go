package agent

import (
	"sync"
	"time"

	"github.com/pyroscope-io/pyroscope/pkg/structs/transporttrie"
	"github.com/pyroscope-io/pyroscope/pkg/util/throttle"
	"github.com/shirou/gopsutil/process"
)

type resetterShim struct {
	s         Snapshotter
	stopChan  chan struct{}
	resetChan chan struct{}

	logger    Logger
	throttler *throttle.Throttler

	trieMutex sync.Mutex
	tries     map[string]*transporttrie.Trie
}

const errorThrottlerPeriod = 10 * time.Second

func CreateResetter(s Snapshotter, l Logger) Resetter {
	rs := &resetterShim{
		s:         s,
		logger:    l,
		throttler: throttle.New(errorThrottlerPeriod),
		tries:     make(map[string]*transporttrie.Trie),
	}
	return rs
}

func (rs *resetterShim) Stop() error {
	// TODO: implement
	return nil
}

func (rs *resetterShim) Reset() map[string]*transporttrie.Trie {
	m := map[string]*transporttrie.Trie{}
	// TODO: implement

	return m
}

// revive:disable-next-line:cognitive-complexity complexity is fine
// this thing is:
// * a timer

func (rs *resetterShim) takeSnapshots() {
	// TODO: implement
	ticker := time.NewTicker(time.Second / time.Duration(ps.sampleRate))
	defer ticker.Stop()
	for {
		select {
		case <-rs.resetChan:
		case <-rs.stopChan:
			// stop the spies
			return
		case <-ticker.C:
			rs.trieMutex.Lock()
			pidsToRemove := []int{}
			rs.s.Snapshot(func(stack []byte, v uint64, err error) {
				if err != nil {
					if ok, pidErr := process.PidExists(int32(pid)); !ok || pidErr != nil {
						rs.logger.Debugf("error taking snapshot: process doesn't exist?")
						pidsToRemove = append(pidsToRemove, pid)
					} else {
						rs.throttler.Run(func(skipped int) {
							if skipped > 0 {
								rs.logger.Errorf("error taking snapshot: %v, %d messages skipped due to throttling", err, skipped)
							} else {
								rs.logger.Errorf("error taking snapshot: %v", err)
							}
						})
					}
					return
				}
				if len(stack) > 0 {
					ps.tries[ps.appName].Insert(stack, v, true)
				}
			})
			for _, pid := range pidsToRemove {
				delete(ps.spies, pid)
			}
			rs.trieMutex.Unlock()
		}
	}
}

// ps.appName
// ps.sampleRate
// ps.spies
// ps.tries
