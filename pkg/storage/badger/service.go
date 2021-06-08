package badger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/pyroscope-io/pyroscope/pkg/storage/badger/gcache"
	"github.com/pyroscope-io/pyroscope/pkg/util/timer"
	"github.com/sirupsen/logrus"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

const (
	LRU = gcache.LRU // lru cache
	ARC = gcache.ARC // arc cache
	LFU = gcache.LFU // lfu cache

	// goroutines for flushing cache to badger
	defaultFlushGoroutines = 4
	// the interval time for collecting metrics
	defaultUpdateInterval = time.Second * 5
	// the badger name for files
	defaultBadgerName = "badger"
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

// Transformer for different types
type Transformer interface {
	// Bytes serializes objects before they go into storage
	Bytes(k string, v interface{}) ([]byte, error)
	// FromBytes deserializes object coming from storage
	FromBytes(k string, v []byte) (interface{}, error)
	// New creates a new object
	New() interface{}
}

// Config for badger
type Config struct {
	StoragePath string // the storage path for badger
	Size        int    // the cache size
	Strategy    string // the cache strategy
	NoTruncate  bool   // whether value log files should be truncated to delete corrupt data
	LogLevel    string // the log level for badger
}

// Service for badger with cache
type Service struct {
	config *Config       // the settings for badger
	cache  Cache         // the cache for badger
	db     *badger.DB    // the badger for persistence
	done   chan struct{} // the service is done
}

func (s *Service) newBadger(config *Config) (*badger.DB, error) {
	// mkdir the badger path
	badgerPath := filepath.Join(config.StoragePath, defaultBadgerName)
	err := os.MkdirAll(badgerPath, 0o755)
	if err != nil {
		return nil, err
	}
	// init the badger options
	badgerOptions := badger.DefaultOptions(badgerPath)
	badgerOptions = badgerOptions.WithTruncate(!config.NoTruncate)
	badgerOptions = badgerOptions.WithSyncWrites(false)
	badgerOptions = badgerOptions.WithCompression(options.ZSTD)
	badgerLevel := logrus.ErrorLevel
	if l, err := logrus.ParseLevel(config.LogLevel); err == nil {
		badgerLevel = l
	}
	badgerOptions = badgerOptions.WithLogger(Logger{name: defaultBadgerName, logLevel: badgerLevel})

	// open the badger
	db, err := badger.Open(badgerOptions)
	if err != nil {
		return nil, err
	}
	// start the badger GC
	timer.StartWorker("badger gc", s.done, 5*time.Minute, func() error {
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

	// new a badger
	db, err := s.newBadger(config)
	if err != nil {
		return nil, err
	}
	s.db = db

	// new a cache
	cache, err := gcache.New(
		gcache.WithSize(config.Size),
		gcache.WithStrategy(config.Strategy),
		gcache.WithEvictFunc(s.evictHandler),
	)
	if err != nil {
		return nil, err
	}
	s.cache = cache

	// start a timer to update the metrics periodly
	s.updateMetrics(defaultUpdateInterval)

	return s, nil
}

// update the metrics periodly
func (s *Service) updateMetrics(interval time.Duration) {
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

// Close the badger service
func (s *Service) Close() {
	if s.done != nil {
		close(s.done)
	}

	// flush the cache to badger
	s.flush(defaultFlushGoroutines)

	// close the badger
	if err := s.db.Close(); err != nil {
		logrus.Errorf("close badger: %v", err)
	}
}

// handle the evicted item from cache
func (s *Service) evictHandler(key interface{}, value interface{}) {
	if err := s.doUpdate(key.(string), value); err != nil {
		logrus.Errorf("do update: %v", err)
	}
}

func (s *Service) doUpdate(key string, value interface{}) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte(key), value.([]byte)); err != nil {
			return fmt.Errorf("set entry: %v", err)
		}
		return nil
	})
}

// Get a key from cache or badger
// 1. find dimension from cache
// 2. find dimension from badger and update the cache
// 3. if not found, create and update a new one to cache
func (s *Service) Get(key string, transformer Transformer) (interface{}, error) {
	// find the value from cache first
	value, err := s.cache.Get(key)
	if err != nil {
		return nil, err
	}
	if value != nil {
		return value, nil
	}

	var buf []byte
	// read value from badger
	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
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

	// create the key and value to cache
	if buf == nil {
		newValue := transformer.New()
		// set the key and new value to cache
		s.Set(key, newValue)

		return newValue, nil
	}
	// update the key and value to cache
	value, err = transformer.FromBytes(key, buf)
	if err != nil {
		return nil, fmt.Errorf("deserialize %v: %v", key, err)
	}
	// set the key and new value to cache
	s.Set(key, value)

	return value, nil
}

// Update a key and value to badger
func (s *Service) Update(key string, value interface{}) error {
	return s.doUpdate(key, value)
}

// Set a key and value to cache
func (s *Service) Set(key string, value interface{}) {
	s.cache.Set(key, value)
}

// Del a key from cache and badger
func (s *Service) Del(key string) error {
	// delete a key from cache
	s.cache.Del(key)

	// delete a key from badger
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}
