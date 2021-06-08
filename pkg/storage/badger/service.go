package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/pyroscope-io/pyroscope/pkg/storage/badger/gcache"
	"github.com/pyroscope-io/pyroscope/pkg/util/timer"
	"github.com/sirupsen/logrus"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

const (
	LRU = gcache.LRU
	ARC = gcache.ARC
	LFU = gcache.LFU

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

	storageMissCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "storage_miss_count",
		Help: "The storage miss count",
	})
)

func init() {
	prometheus.MustRegister(cacheHitCount)
	prometheus.MustRegister(cacheMissCount)
	prometheus.MustRegister(cacheHitRate)
	prometheus.MustRegister(storageMissCount)
}

// Config for badger
type Config struct {
	StoragePath string // the storage path for badger
	Size        int    // the cache size
	Strategy    string // the cache strategy
	NoTruncate  bool   // whether value log files should be truncated to delete corrupt data
}

// Cache service
type Service struct {
	config *Config       // the settings for badger
	cache  Cache         // the cache for badger
	db     *badger.DB    // the badger for persistence
	done   chan struct{} // the service is done
}

func newBadger(config *Config, name string) (*badger.DB, error) {
	// mkdir the badger path
	badgerPath := filepath.Join(cfg.StoragePath, name)
	err := os.MkdirAll(badgerPath, 0o755)
	if err != nil {
		return nil, err
	}
	// init the badger options
	badgerOptions := badger.DefaultOptions(badgerPath)
	badgerOptions = badgerOptions.WithTruncate(!cfg.BadgerNoTruncate)
	badgerOptions = badgerOptions.WithSyncWrites(false)
	badgerOptions = badgerOptions.WithCompression(options.ZSTD)
	badgerLevel := logrus.ErrorLevel
	if l, err := logrus.ParseLevel(cfg.BadgerLogLevel); err == nil {
		badgerLevel = l
	}
	badgerOptions = badgerOptions.WithLogger(badgerLogger{name: name, logLevel: badgerLevel})

	// open the badger
	db, err := badger.Open(badgerOptions)
	if err != nil {
		return nil, err
	}
	// start the badger GC
	timer.StartWorker("badger gc", make(chan struct{}), 5*time.Minute, func() error {
		return db.RunValueLogGC(0.7)
	})

	return db, err
}

// NewService returns a badger service which supports the cache
func NewService(config *Config) (*Service, error) {
	// new a cache service
	s := &Service{
		config: config,
		done:   make(chan struct{}),
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
// func (s *Service) flush(goroutines int) {
// 	wb := s.db.NewWriteBatch()
// 	defer wb.Cancel()

// 	// all the cache items
// 	keys, values := s.cache.GetAll()
// 	if len(keys) == 0 {
// 		return
// 	}

// 	var wg sync.WaitGroup
// 	for i := 0; i < goroutines; i++ {
// 		wg.Add(1)

// 		num := len(keys) / goroutines

// 		start, end := i*num, (i+1)*num
// 		go func(l, r int) {
// 			defer wg.Done()

// 			wb := s.db.NewWriteBatch()
// 			defer wb.Cancel()

// 			for i := l; i < r; i++ {
// 				if err := wb.Set(keys[i].([]byte), values[i].([]byte)); err != nil {
// 					logrus.Errorf("write batch set: %v", err)
// 				}
// 			}
// 			wb.Flush()
// 		}(start, end)
// 	}

// 	// wait until the goroutines are done
// 	wg.Wait()
// }

// Close the cache service
// func (s *Service) Close() {
// 	if s.done != nil {
// 		close(s.done)
// 	}

// 	// flush the cache to badger
// 	s.flush(flushGoroutines)

// 	// close the badger
// 	if err := s.db.Close(); err != nil {
// 		logrus.Errorf("close badger: %v", err)
// 	}
// }

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
	// find the value from cache first
	val, err := s.cache.Get(key)
	if err != nil {
		return nil, err
	}
	if val != nil {
		return val, nil
	}

	var buf []byte
	// read value from badger
	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key.([]byte))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("read from badger: %v", err)
		}
		if err := item.Value(func(val []byte) error {
			buf = append([]byte{}, val...)
			return nil
		}); err != nil {
			return fmt.Errorf("read item value: %v", err)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("badger view: %v", err)
	}

	// if not found from badger, new an object
	if buf == nil {
		storageMissCount.Add(1)

	}
}

// Set a key and value to cache
func (s *Service) Set(key interface{}, value interface{}) {
	s.cache.Set(key, value)
}

// Del a key from cache
func (s *Service) Del(key interface{}) bool {
	return s.cache.Del(key)
}
