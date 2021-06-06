package ristretto

import (
	deps "github.com/dgraph-io/ristretto"
)

type Cache struct {
	deps.Cache
}

func NewCache() *Cache {
	return nil
}

func (s *Cache) Get(key interface{}) interface{} {
	return nil
}

func (s *Cache) Set(key interface{}, value interface{}) {

}
