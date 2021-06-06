package gcache

type CacheOption func(*cacheOptions)

type cacheOptions struct {
	checkExpired bool                           // if check expiration
	strategy     string                         // lfu, lru and arc
	size         int                            // the cache size
	evict        func(interface{}, interface{}) // evict function
}

func WithStrategy(strategy string) CacheOption {
	return func(o *cacheOptions) {
		o.strategy = strategy
	}
}

func WithSize(size int) CacheOption {
	return func(o *cacheOptions) {
		o.size = size
	}
}

func WithEvictFunc(evict func(interface{}, interface{})) CacheOption {
	return func(o *cacheOptions) {
		o.evict = evict
	}
}

func WithCheckExpired(checkExpired bool) CacheOption {
	return func(o *cacheOptions) {
		o.checkExpired = checkExpired
	}
}
