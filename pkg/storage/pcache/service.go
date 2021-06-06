package pcache

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/pyroscope-io/pyroscope/pkg/storage/pcache/gcache"
	"github.com/sirupsen/logrus"

	"github.com/dgraph-io/badger/v2"
)

const (
	LRU = gcache.LRU
	LFU = gcache.LFU
	ARC = gcache.ARC

	collectInterval = time.Second * 5
	flushGoroutines = 4
)

var (
	cacheHitCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_hit_count",
		Help: "The cache hit count",
	})

	cacheMissCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_miss_count",
		Help: "The cache miss count",
	})

	cacheHitRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "cache_hit_rate",
		Help: "The cache hit rate",
	})
)

func init() {
	prometheus.MustRegister(cacheHitCount)
	prometheus.MustRegister(cacheMissCount)
	prometheus.MustRegister(cacheHitRate)
}

// Cache service
type Service struct {
	cache Cache         // the cache handler
	db    *badger.DB    // the badger handler
	done  chan struct{} // the service is done
}

// NewService returns a new cache service
// strategy: LFU, LRU or ARC
func NewService(db *badger.DB, strategy string, size int) (*Service, error) {
	if db == nil {
		return nil, errors.New("badger db is nil")
	}

	// new a cache service
	s := &Service{
		db:   db,
		done: make(chan struct{}),
	}

	// init a special cache
	cache, err := gcache.New(
		gcache.WithSize(size),
		gcache.WithStrategy(strategy),
		gcache.WithEvictFunc(s.evictHandler),
	)
	if err != nil {
		return nil, err
	}
	s.cache = cache

	// start a timer to collect the metrics periodly
	s.collect(collectInterval)

	return s, nil
}

// collect the metrics periodly
func (s *Service) collect(interval time.Duration) {
	go func() {
		ticker := time.NewTimer(interval)
		defer ticker.Stop()

		select {
		case <-s.done:
			return

		case <-ticker.C:
			// update the metrics for hit, miss, hit rate
			cacheHitCount.Set(float64(s.cache.HitCount()))
			cacheMissCount.Set(float64(s.cache.MissCount()))
			cacheHitRate.Set(s.cache.HitRate())

			// reset the timer
			ticker.Reset(interval)
		}
	}()
}

// flush the cache items to badger
func (s *Service) flush(goroutines int) {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()

	// all the cache items
	keys, values := s.cache.GetAll()
	if len(keys) == 0 {
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		num := len(keys) / goroutines

		start, end := i*num, (i+1)*num
		go func(l, r int) {
			defer wg.Done()

			wb := s.db.NewWriteBatch()
			defer wb.Cancel()

			for i := l; i < r; i++ {
				if err := wb.Set(keys[i].([]byte), values[i].([]byte)); err != nil {
					logrus.Errorf("write batch set: %v", err)
				}
			}
			wb.Flush()
		}(start, end)
	}

	// wait until the goroutines are done
	wg.Wait()
}

// Close the cache service
func (s *Service) Close() {
	if s.done != nil {
		close(s.done)
	}

	// flush the cache to badger
	s.flush(flushGoroutines)

	// close the badger
	if err := s.db.Close(); err != nil {
		logrus.Errorf("close badger: %v", err)
	}
}

// handle the evicted item from cache
func (s *Service) evictHandler(key interface{}, value interface{}) {
	if err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key.([]byte), value.([]byte)); err != nil {
			return fmt.Errorf("transaction set entry: %v", err)
		}
		return nil
	}); err != nil {
		logrus.Errorf("db update: %v", err)
	}
}

// Get a key from cache
func (s *Service) Get(key interface{}) (interface{}, error) {
	return s.cache.Get(key)
}

// Set a key and value to cache
func (s *Service) Set(key interface{}, value interface{}) {
	s.cache.Set(key, value)
}

// Del a key from cache
func (s *Service) Del(key interface{}) bool {
	return s.cache.Del(key)
}
