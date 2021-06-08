package badger

// Cache for the badger
type Cache interface {
	Get(key interface{}) (interface{}, error)
	GetAll() ([]interface{}, []interface{})
	Set(key interface{}, value interface{})
	Del(key interface{}) bool
	Len() int
	HitCount() uint64
	MissCount() uint64
	LookupCount() uint64
	HitRate() float64
}
