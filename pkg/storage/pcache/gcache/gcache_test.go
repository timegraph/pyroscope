package gcache

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func evict(key interface{}, value interface{}) {
	log.Printf("key: %v, value: %v", key, value)
}

func TestCacheWithLRU(t *testing.T) {
	size := 10
	cache, err := New(
		WithSize(size),
		WithStrategy(LRU),
		WithEvictFunc(evict),
		WithCheckExpired(false),
	)
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	for i := 0; i < size+5; i++ {
		cache.Set(fmt.Sprintf("%d", i), i)
	}
	for i := 0; i < size; i++ {
		key := fmt.Sprintf("%d", i)
		if _, err := cache.Get(key); err != nil {
			t.Fatalf("cache get %s: %v", key, err)
		}
	}
	assert.Equal(t, uint64(5), cache.HitCount())
	assert.Equal(t, uint64(5), cache.MissCount())
}
