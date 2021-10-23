package pbuf

// import "github.com/pyroscope-io/pyroscope/pkg/agent/spy"

// type cacheKey []int64

// type cacheEntry struct {
// 	key cacheKey
// 	val *spy.Labels
// }
// type cache struct {
// 	data []*cacheEntry
// }

// func newCache() *cache {
// 	return &cache{
// 		data: []*cacheEntry{},
// 	}
// }

// func getCacheKey(l []*Label) cacheKey {
// 	r := []int64{}
// 	for _, x := range l {
// 		if x.Str != 0 {
// 			r = append(r, x.Key, x.Str)
// 		}
// 	}
// 	return r
// }

// func eq(a, b []int64) bool {
// 	if len(a) != len(b) {
// 		return false
// 	}
// 	for i, v := range a {
// 		if v != b[i] {
// 			return false
// 		}
// 	}
// 	return true
// }

// func (c *cache) pprofLabelsToSpyLabels(x *Profile, pprofLabels []*Label) *spy.Labels {
// 	k := getCacheKey(pprofLabels)
// 	for _, e := range c.data {
// 		if eq(e.key, k) {
// 			return e.val
// 		}
// 	}

// 	l := spy.NewLabels()
// 	for _, pl := range pprofLabels {
// 		if pl.Str != 0 {
// 			l.Set(x.StringTable[pl.Key], x.StringTable[pl.Str])
// 		}
// 	}
// 	newVal := &cacheEntry{
// 		key: k,
// 		val: l,
// 	}
// 	c.data = append(c.data, newVal)
// 	return l
// }
