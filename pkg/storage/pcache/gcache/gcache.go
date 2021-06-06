package gcache

import (
	"errors"

	deps "github.com/bluele/gcache"
)

const (
	LRU = deps.TYPE_LRU
	LFU = deps.TYPE_LFU
	ARC = deps.TYPE_ARC
)

type Cache struct {
	cache   deps.Cache
	options *cacheOptions
}

func New(opts ...CacheOption) (*Cache, error) {
	options := &cacheOptions{}
	for _, o := range opts {
		o(options)
	}

	if options.size == 0 {
		return nil, errors.New("cache size is zero")
	}
	builder := deps.New(options.size)
	switch options.strategy {
	case LRU:
		builder = builder.LRU()
	case LFU:
		builder = builder.LFU()
	case ARC:
		builder = builder.ARC()
	default:
		builder = builder.Simple()
	}
	if options.evict != nil {
		builder = builder.EvictedFunc(options.evict)
	}

	return &Cache{
		cache:   builder.Build(),
		options: options,
	}, nil
}

func (s *Cache) Get(key interface{}) (interface{}, error) {
	val, err := s.cache.Get(key)
	if err != nil {
		if err == deps.KeyNotFoundError {
			return nil, nil
		}
		return nil, err
	}
	return val, nil
}

func (s *Cache) Set(key interface{}, value interface{}) {
	s.cache.Set(key, value)
}

func (s *Cache) Del(key interface{}) bool {
	return s.cache.Remove(key)
}

func (s *Cache) Len() int {
	return s.cache.Len(s.options.checkExpired)
}

func (s *Cache) HitCount() uint64 {
	return s.cache.HitCount()
}

func (s *Cache) MissCount() uint64 {
	return s.cache.MissCount()
}

func (s *Cache) LookupCount() uint64 {
	return s.cache.LookupCount()
}

func (s *Cache) HitRate() float64 {
	return s.cache.HitRate()
}

func (s *Cache) GetAll() ([]interface{}, []interface{}) {
	keys, values := []interface{}{}, []interface{}{}

	for key, value := range s.cache.GetALL(s.options.checkExpired) {
		keys = append(keys, key)
		values = append(values, value)
	}
	return keys, values
}
