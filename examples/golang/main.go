package main

import (
	"context"
	"runtime/pprof"

	"github.com/pyroscope-io/pyroscope/pkg/agent/profiler"
)

//go:noinline
func work(n int) {
	// revive:disable:empty-block this is fine because this is a example app, not real production code
	for i := 0; i < n; i++ {
	}
	// revive:enable:empty-block
}

//go:noinline
func work2(n int) {
	// revive:disable:empty-block this is fine because this is a example app, not real production code
	for i := 0; i < n; i++ {
	}
	// revive:enable:empty-block
}

func fastFunction(c context.Context) {
	pprof.Do(c, pprof.Labels("function", "fast"), func(c context.Context) {
		go work2(200)
		work(2000)
	})
}

func slowFunction(c context.Context) {
	pprof.Do(c, pprof.Labels("function", "slow"), func(c context.Context) {
		go work2(800)
		work(8000)
	})
}

func main() {
	profiler.Start(profiler.Config{
		ApplicationName: "simple.golang.app",
		ServerAddress:   "http://localhost:4040", // this will run inside docker-compose, hence `pyroscope` for hostname
	})
	pprof.Do(context.Background(), pprof.Labels("function", "main", "foo", "bar"), func(c context.Context) {
		for {
			fastFunction(c)
			slowFunction(c)
		}
	})
}
