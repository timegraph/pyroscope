package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/pyroscope-io/pyroscope/pkg/util/bytesize"
	"github.com/pyroscope-io/pyroscope/pkg/util/disk"
	"github.com/pyroscope-io/pyroscope/pkg/util/slices"

	origin "github.com/dgraph-io/badger/v2"
	"github.com/pyroscope-io/pyroscope/pkg/config"
	"github.com/pyroscope-io/pyroscope/pkg/storage/badger"
	"github.com/pyroscope-io/pyroscope/pkg/storage/dict"
	"github.com/pyroscope-io/pyroscope/pkg/storage/dimension"
	"github.com/pyroscope-io/pyroscope/pkg/storage/segment"
	"github.com/pyroscope-io/pyroscope/pkg/storage/tree"
	"github.com/pyroscope-io/pyroscope/pkg/structs/merge"
	"github.com/sirupsen/logrus"
)

var errOutOfSpace = errors.New("out of space")

var (
	storagePutCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_put_count",
		Help: "The storage put count",
	}, []string{"name"})

	storageGetCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_get_count",
		Help: "The storage get count",
	}, []string{"name"})
)

func init() {
	prometheus.MustRegister(storagePutCount)
	prometheus.MustRegister(storageGetCount)
}

const (
	Dimension = "i:"
	Segment   = "s:"
	DifPrefix = "e:"
	Dict      = "d:"
	Tree      = "t:"
)

type Storage struct {
	mutex   sync.Mutex
	config  *config.Server
	service *badger.Service
}

func New(config *config.Server) (*Storage, error) {
	// new a badger service with cache for storage
	service, err := badger.NewService(&badger.Config{
		Size:        config.CacheSize,
		StoragePath: config.StoragePath,
		NoTruncate:  config.BadgerNoTruncate,
		LogLevel:    config.BadgerLogLevel,
		Strategy:    config.CacheStrategy,
	})
	if err != nil {
		return nil, err
	}

	s := &Storage{
		config:  config,
		service: service,
	}

	// set the serializer
	service.SetSerializer(s.serialize)

	return s, nil
}

type PutInput struct {
	StartTime       time.Time
	EndTime         time.Time
	Key             *Key
	Val             *tree.Tree
	SpyName         string
	SampleRate      uint32
	Units           string
	AggregationType string
}

// serialize the different structure to bytes
func (s *Storage) serialize(k string, v interface{}) ([]byte, []byte, error) {
	switch v := v.(type) {
	case *dimension.Dimension:
		dm, err := v.Bytes()
		if err != nil {
			return nil, nil, err
		}
		return []byte(Dimension + k), dm, nil
	case *dict.Dict:
		di, err := v.Bytes()
		if err != nil {
			return nil, nil, err
		}
		return []byte(Dict + k), di, nil
	case *segment.Segment:
		se, err := v.Bytes()
		if err != nil {
			return nil, nil, err
		}
		return []byte(Segment + k), se, nil
	case *tree.Tree:
		// parse the main key from tree key
		dictKey := FromTreeToMainKey(k)

		// get dict from cache or badger
		di, err := s.getDict(dictKey)
		if err != nil {
			return nil, nil, err
		}
		// serialize the tree
		tr, err := v.Bytes(di, s.config.MaxNodesSerialization)
		if err != nil {
			return nil, nil, err
		}
		return []byte(Tree + k), tr, nil
	default:
		return []byte(k), v.([]byte), nil
	}
}

// getSegment find a segment with key from cache
func (s *Storage) getSegment(key string) (*segment.Segment, error) {
	// diff the segment and dict key
	key = DifPrefix + key
	// find segment from cache or badger
	val, err := s.service.Get(Segment, key, func(b []byte) (interface{}, error) {
		var ns *segment.Segment
		if b == nil {
			// create a new segment
			ns = segment.New()
		} else {
			var err error
			// deserialize from bytes to segment
			ns, err = segment.FromBytes(b)
			if err != nil {
				return nil, err
			}
		}
		// set the key and new segment to cache
		s.service.Set(key, ns)

		return ns, nil
	})
	if err != nil {
		return nil, fmt.Errorf("find segment %v: %v", key, err)
	}

	se, ok := val.(*segment.Segment)
	if !ok {
		return nil, fmt.Errorf("must be segment object: %v, %T", key, val)
	}
	return se, nil
}

// getDimension find a dimension with key from cache
func (s *Storage) getDimension(key string) (*dimension.Dimension, error) {
	// find dimension from cache or badger
	val, err := s.service.Get(Dimension, key, func(b []byte) (interface{}, error) {
		var nd *dimension.Dimension
		if b == nil {
			// create a new dimension
			nd = dimension.New()
		} else {
			var err error
			// deserialize from bytes to dimension
			nd, err = dimension.FromBytes(b)
			if err != nil {
				return nil, err
			}
		}
		// set the key and new dimension to cache
		s.service.Set(key, nd)

		return nd, nil
	})
	if err != nil {
		return nil, err
	}

	di, ok := val.(*dimension.Dimension)
	if !ok {
		return nil, fmt.Errorf("must be dimension object: %v, %T", key, val)
	}
	return di, nil
}

// getDict find a dict with key from cache
func (s *Storage) getDict(key string) (*dict.Dict, error) {
	// get dict from cache or badger
	val, err := s.service.Get(Dict, key, func(b []byte) (interface{}, error) {
		var nd *dict.Dict
		if b == nil {
			// create a new dict
			nd = dict.New()
		} else {
			var err error
			// deserialize from bytes to dict
			nd, err = dict.FromBytes(b)
			if err != nil {
				return nil, err
			}
		}
		// set the key and new dict to cache
		s.service.Set(key, nd)

		return nd, nil
	})
	if err != nil {
		return nil, err
	}

	di, ok := val.(*dict.Dict)
	if !ok {
		return nil, fmt.Errorf("must be dict object: %v, %T", key, val)
	}
	return di, nil
}

// getTree find a tree with key from cache
func (s *Storage) getTree(key string) (*tree.Tree, error) {
	// get tree from cache or badger
	val, err := s.service.Get(Tree, key, func(b []byte) (interface{}, error) {
		var nt *tree.Tree
		if b == nil {
			// create a new tree
			nt = tree.New()
		} else {
			// parse the main key from tree key
			dictKey := FromTreeToMainKey(key)

			// get dict from cache or badger
			di, err := s.getDict(dictKey)
			if err != nil {
				return nil, err
			}

			// deserialize from bytes to tree
			nt, err = tree.FromBytes(di, b)
			if err != nil {
				return nil, err
			}
		}
		// set the key and new tree to cache
		s.service.Set(key, nt)

		return nt, nil
	})
	if err != nil {
		return nil, err
	}

	tr, ok := val.(*tree.Tree)
	if !ok {
		return nil, fmt.Errorf("must be tree object: %v, %T", key, val)
	}
	return tr, nil
}

func (s *Storage) Put(pi *PutInput) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if the disk size is out of space
	freeSpace, _ := disk.FreeSpace(s.config.StoragePath)
	if freeSpace < s.config.OutOfSpaceThreshold {
		return errOutOfSpace
	}

	logrus.WithFields(logrus.Fields{
		"startTime":       pi.StartTime.String(),
		"endTime":         pi.EndTime.String(),
		"key":             pi.Key.Normalized(),
		"samples":         pi.Val.Samples(),
		"units":           pi.Units,
		"aggregationType": pi.AggregationType,
	}).Debug("storage.Put")

	// update the labels to badger
	for k, v := range pi.Key.labels {
		kk := "l:" + k
		kv := "v:" + k + ":" + v
		if err := s.service.Update(kk, []byte{}); err != nil {
			return fmt.Errorf("update %v: %v", kk, err)
		}
		if err := s.service.Update(kv, []byte{}); err != nil {
			return fmt.Errorf("update %v: %v", kv, err)
		}
	}

	// segement key
	sk := pi.Key.SegmentKey()

	// update the dimesion and sort the keys
	for k, v := range pi.Key.labels {
		key := k + ":" + v
		// find dimension from cache or badger
		di, err := s.getDimension(key)
		if err != nil {
			return fmt.Errorf("find dimension %v: %v", key, err)
		}
		di.Insert([]byte(sk))
	}

	// find segment from cache or badger
	se, err := s.getSegment(sk)
	if err != nil {
		logrus.Errorf("find segment %v: %v", sk, err)
		return err
	}
	// set the metadata to segement
	se.SetMetadata(pi.SpyName, pi.SampleRate, pi.Units, pi.AggregationType)

	// the samples from segment
	samples := pi.Val.Samples()
	se.Put(pi.StartTime, pi.EndTime, samples, func(depth int, t time.Time, r *big.Rat, addons []segment.Addon) {
		key := pi.Key.TreeKey(depth, t)

		// get tree from cache or badger
		mainTree, err := s.getTree(key)
		if err != nil {
			logrus.Errorf("find tree %v: %v", key, err)
			return
		}

		cloneTree := pi.Val.Clone(r)
		for _, addon := range addons {
			addonKey := pi.Key.TreeKey(addon.Depth, addon.T)
			// get tree from cache or badger
			addonTree, err := s.getTree(addonKey)
			if err != nil {
				logrus.Errorf("find tree %v: %v", addonKey, err)
				return
			}
			// merge the clone and addon tree
			cloneTree.Merge(addonTree)
		}
		if mainTree != nil {
			// merge the main and clone tree
			mainTree.Merge(cloneTree)

			s.service.Set(key, mainTree)
		} else {
			s.service.Set(key, cloneTree)
		}
	})

	// update the key and value to cache
	s.service.Set(DifPrefix+sk, se)

	// update the metrics
	storagePutCount.WithLabelValues(pi.SpyName).Add(1)

	return nil
}

type GetInput struct {
	StartTime time.Time
	EndTime   time.Time
	Key       *Key
}

type GetOutput struct {
	Tree       *tree.Tree
	Timeline   *segment.Timeline
	SpyName    string
	SampleRate uint32
	Units      string
}

func (s *Storage) Get(gi *GetInput) (*GetOutput, error) {
	logrus.WithFields(logrus.Fields{
		"startTime": gi.StartTime.String(),
		"endTime":   gi.EndTime.String(),
		"key":       gi.Key.Normalized(),
	}).Debug("storage.Get")

	dimensions := []*dimension.Dimension{}
	// find the dimensions for the labels
	for k, v := range gi.Key.labels {
		key := k + v

		// find dimension from cache or badger
		di, err := s.getDimension(key)
		if err != nil {
			return nil, fmt.Errorf("find dimension %v: %v", key, err)
		}
		dimensions = append(dimensions, di)
	}

	// generate a timeline for start and end time
	timeline := segment.GenerateTimeline(gi.StartTime, gi.EndTime)

	var writeBytes uint64
	// default aggregation type is sum
	aggregationType := "sum"

	var lastSegment *segment.Segment
	// segment keys from dimention
	segmentKeys := dimension.Intersection(dimensions...)

	tries := []merge.Merger{}
	for _, sk := range segmentKeys {
		// TODO: refactor, store `Key`s in dimensions

		// parse the segement key
		parsedKey, err := ParseKey(string(sk))
		if err != nil {
			return nil, fmt.Errorf("parse key: %v: %v", string(sk), err)
		}

		// get segment key
		key := parsedKey.SegmentKey()
		// find segment from cache or badger
		se, err := s.getSegment(key)
		if err != nil {
			return nil, fmt.Errorf("find segment %v: %v", key, err)
		}
		if se.AggregationType() == "average" {
			aggregationType = "average"
		}

		// point to the last segment
		lastSegment = se

		// populate the timeline with current found segment
		timeline.PopulateTimeline(se)

		// find and merge the trees for the segment
		se.Get(gi.StartTime, gi.EndTime, func(depth int, samples, writes uint64, t time.Time, r *big.Rat) {
			tk := parsedKey.TreeKey(depth, t)

			// find tree from cache or badger
			tr, err := s.getTree(tk)
			if err != nil {
				logrus.Errorf("find tree %v: %v", tk, err)
				return
			}

			// TODO: these clones are probably are not the most efficient way of doing this
			// instead this info should be passed to the merger function imo
			tries = append(tries, merge.Merger(tr.Clone(r)))
			// update the total bytes
			writeBytes += writes
		})
	}

	// merge the tries concurrently
	mergedTrie := merge.MergeTriesConcurrently(runtime.NumCPU(), tries...)
	if mergedTrie == nil {
		return nil, nil
	}

	tr := mergedTrie.(*tree.Tree)
	if writeBytes > 0 && aggregationType == "average" {
		tr = tr.Clone(big.NewRat(1, int64(writeBytes)))
	}

	out := &GetOutput{
		Tree:       tr,
		Timeline:   timeline,
		SpyName:    lastSegment.SpyName(),
		SampleRate: lastSegment.SampleRate(),
		Units:      lastSegment.Units(),
	}

	// update the metrics
	storageGetCount.WithLabelValues(lastSegment.SpyName()).Add(1)

	return out, nil
}

type DeleteInput struct {
	StartTime time.Time
	EndTime   time.Time
	Key       *Key
}

func (s *Storage) Delete(di *DeleteInput) error {
	logrus.WithFields(logrus.Fields{
		"startTime": di.StartTime.String(),
		"endTime":   di.EndTime.String(),
		"key":       di.Key.Normalized(),
	}).Info("storage.Delete")

	dimensions := []*dimension.Dimension{}
	// find the dimensions for the labels
	for k, v := range di.Key.labels {
		key := k + ":" + v

		// find dimension from cache or badger
		di, err := s.getDimension(key)
		if err != nil {
			return fmt.Errorf("find dimension %v: %v", key, err)
		}
		dimensions = append(dimensions, di)
	}

	// keys from dimensions
	dimensionKeys := dimension.Intersection(dimensions...)
	for _, dk := range dimensionKeys {
		// TODO: refactor, store `Key`s in dimensions

		// parse the dimension key
		key, err := ParseKey(string(dk))
		if err != nil {
			return fmt.Errorf("parse key: %v: %v", string(dk), err)
		}

		// get segment key
		sk := key.SegmentKey()
		// find segment from cache or badger
		se, err := s.getSegment(sk)
		if err != nil {
			return fmt.Errorf("find segment %v: %v", sk, err)
		}

		// delete the trees for the segment
		se.Get(di.StartTime, di.EndTime, func(depth int, samples, writes uint64, t time.Time, r *big.Rat) {
			tk := key.TreeKey(depth, t)
			// delete the tress from cache and badger
			s.service.Del(Tree, tk)
			// delete the dict from cache and badger
			s.service.Del(Dict, FromTreeToMainKey(tk))
		})

		// delete the segment from cache and badger
		s.service.Del(Segment, DifPrefix+key.SegmentKey())
	}

	// delete the dimensions from cache and badger
	for k, v := range di.Key.labels {
		s.service.Del(Dimension, k+":"+v)
	}

	return nil
}

// Close the storage
func (s *Storage) Close() error {
	if s.service != nil {
		s.service.Close()
	}
	return nil
}

func (s *Storage) GetKeys(cb func(k string) bool) error {
	if err := s.service.View(func(txn *origin.Txn) error {
		opts := origin.DefaultIteratorOptions
		opts.Prefix = []byte("l:")
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			if !cb(string(key[2:])) {
				return nil
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Storage) GetValues(k string, cb func(v string) bool) error {
	if err := s.service.View(func(txn *origin.Txn) error {
		opts := origin.DefaultIteratorOptions
		opts.Prefix = []byte("v:" + k + ":")
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().Key()
			pos := bytes.LastIndex(key, []byte{':'})

			val := string(key[pos:])
			if k != "__name__" || !slices.StringContains(s.config.HideApplications, val) {
				if !cb(val) {
					return nil
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Storage) CacheStats() map[string]interface{} {
	return map[string]interface{}{
		"cache": s.service.Len(),
	}
}

func (s *Storage) DiskUsage() map[string]bytesize.ByteSize {
	res := map[string]bytesize.ByteSize{}
	res["cache"] = dirSize(s.config.StoragePath)
	return res
}

func dirSize(path string) (result bytesize.ByteSize) {
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			result += bytesize.ByteSize(info.Size())
		}
		return nil
	})
	return
}
