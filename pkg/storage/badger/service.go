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
	// goroutines for flushing cache to badger
	defaultFlushGoroutines = 4
	// the interval time for collecting metrics
	defaultUpdateInterval = time.Second
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
)

func init() {
	prometheus.MustRegister(cacheHitCount)
	prometheus.MustRegister(cacheMissCount)
	prometheus.MustRegister(cacheHitRate)
}

// Config for badger
type Config struct {
	StoragePath string // the storage path for badger
	Size        int    // the cache size
	Strategy    string // the cache strategy
	NoTruncate  bool   // whether value log files should be truncated to delete corrupt data
	LogLevel    string // the log level for badger
}

// CacheItem for eviction
type CacheItem struct {
	key   interface{}
	value interface{}
}

// Service for badger with cache
type Service struct {
	config   *Config         // the settings for badger
	cache    Cache           // the cache for badger
	db       *badger.DB      // the badger for persistence
	eviction chan *CacheItem // the channel for eviction
	done     chan struct{}   // the service is done

	// serialize different structure to bytes
	serializer func(key string, value interface{}) ([]byte, []byte, error)
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

	// start a timer for the badger GC
	timer.StartWorker("badger gc", s.done, 5*time.Minute, func() error {
		return db.RunValueLogGC(0.7)
	})

	return db, err
}

// NewService returns a badger service which supports the cache
func NewService(config *Config) (*Service, error) {
	// new a cache service
	s := &Service{
		config:   config,
		done:     make(chan struct{}),
		eviction: make(chan *CacheItem, 1024),
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

	// start to handle the eviction
	go s.handleEviction()

	// start a timer to update the metrics periodly
	timer.StartWorker("metrics", s.done, defaultUpdateInterval, func() error {
		// update the metrics for hit, miss, hit rate
		cacheHitCount.Set(float64(s.cache.HitCount()))
		cacheMissCount.Set(float64(s.cache.MissCount()))
		cacheHitRate.Set(s.cache.HitRate())
		return nil
	})

	return s, nil
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
			// new a write batch for the goroutines
			for i := l; i < r; i++ {
				k := keys[i].(string)
				key, value, err := s.serializer(k, values[i])
				if err != nil {
					logrus.Errorf("serialize: %v", err)
					continue
				}
				if err := wb.Set(key, value); err != nil {
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

// handleEviction save the evicted item to badger
func (s *Service) handleEviction() {
	for {
		select {
		case <-s.done:
			return
		case v := <-s.eviction:
			if err := s.doUpdate(v.key.(string), v.value); err != nil {
				logrus.Errorf("do update for eviction: %v", err)
			}
		}
	}
}

// handle the evicted item from cache, can't operate the cache in the evict handler
func (s *Service) evictHandler(key interface{}, value interface{}) {
	s.eviction <- &CacheItem{key: key, value: value}
}

func (s *Service) doUpdate(k string, value interface{}) error {
	key, buf, err := s.serializer(k, value)
	if err != nil {
		return fmt.Errorf("serialize: %v", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, buf); err != nil {
			return fmt.Errorf("set entry: %v", err)
		}
		return nil
	})
}

// SetSerializer update the serializer for service
func (s *Service) SetSerializer(serializer func(string, interface{}) ([]byte, []byte, error)) {
	s.serializer = serializer
}

// Get a key from cache or badger
func (s *Service) Get(prefix string, key string, upset func([]byte) (interface{}, error)) (interface{}, error) {
	// 1. find the key from cache
	value, err := s.cache.Get(key)
	if err != nil {
		return nil, err
	}
	if value != nil {
		return value, nil
	}

	// 2. find the key from badger
	data, err := s.Query(prefix + key)
	if err != nil {
		return nil, fmt.Errorf("query badger %v: %v", key, err)
	}

	// create or update the key and value to cache
	return upset(data)
}

// Set a key and value to cache
func (s *Service) Set(key string, value interface{}) {
	s.cache.Set(key, value)
}

// Del a key from cache and badger
func (s *Service) Del(prefix string, key string) error {
	// delete a key from cache
	s.cache.Del(key)

	// delete a key from badger
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(prefix + key))
	})
}

// Len returns the current size of cache
func (s *Service) Len() int {
	return s.cache.Len()
}

// Query the badger with the key
func (s *Service) Query(key string) ([]byte, error) {
	var data []byte
	// 2. find dimension from badger
	if err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("read from badger: %v", err)
		}
		if err := item.Value(func(val []byte) error {
			data = append([]byte{}, val...)
			return nil
		}); err != nil {
			return fmt.Errorf("read item value: %v", err)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("badger view: %v", err)
	}

	return data, nil
}

// Update a key and value to badger
func (s *Service) Update(key string, value interface{}) error {
	return s.doUpdate(key, value)
}

// View iterate the key and value from badger
func (s *Service) View(fn func(txn *badger.Txn) error) error {
	return s.db.View(fn)
}
