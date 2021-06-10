package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"time"

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

var errClosing = errors.New("the db is in closing state")
var errOutOfSpace = errors.New("running out of space")

type Storage struct {
	config  *config.Server
	service *badger.Service
}

func New(config *config.Server) (*Storage, error) {
	// new a badger service with cache for storage
	service, err := badger.NewService(&badger.Config{
		Size:        1024 * 1024,
		StoragePath: config.StoragePath,
		NoTruncate:  config.BadgerNoTruncate,
		LogLevel:    config.BadgerLogLevel,
		Strategy:    badger.LFU,
	})
	if err != nil {
		return nil, err
	}

	return &Storage{
		config:  config,
		service: service,
	}, nil
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

// getSegment find a segment with key from cache
func (s *Storage) getSegment(key string) (*segment.Segment, error) {
	// find segment from cache or badger
	val, err := s.service.Get(key, func(b []byte) (interface{}, error) {
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
		return nil, errors.New("must be segment object")
	}
	return se, nil
}

// getDimension find a dimension with key from cache
func (s *Storage) getDimension(key string) (*dimension.Dimension, error) {
	// find dimension from cache or badger
	val, err := s.service.Get(key, func(b []byte) (interface{}, error) {
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
		return nil, errors.New("must be dimension object")
	}
	return di, nil
}

// getDict find a dict with key from cache
func (s *Storage) getDict(key string) (*dict.Dict, error) {
	// get dict from cache or badger
	val, err := s.service.Get(key, func(b []byte) (interface{}, error) {
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
		return nil, errors.New("must be dict object")
	}
	return di, nil
}

// getTree find a tree with key from cache
func (s *Storage) getTree(key string) (*tree.Tree, error) {
	// get tree from cache or badger
	val, err := s.service.Get(key, func(b []byte) (interface{}, error) {
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
		return nil, errors.New("must be tree object")
	}
	return tr, nil
}

func (s *Storage) Put(pi *PutInput) error {
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
	}).Info("storage.Put")

	// update the labels to badger
	for k, v := range pi.Key.labels {
		kk := "l:" + k
		kv := "v:" + k + ":" + v
		if err := s.service.Update(kk, []byte{}); err != nil {
			return fmt.Errorf("update badger: %v", err)
		}
		if err := s.service.Update(kv, []byte{}); err != nil {
			return fmt.Errorf("update badger: %v", err)
		}
	}

	// segement key
	sk := pi.Key.SegmentKey()

	var err error
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
	s.service.Set(sk, se)

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
	}).Info("storage.Get")

	dimensions := []*dimension.Dimension{}
	// find the dimensions for the labels
	for k, v := range gi.Key.labels {
		key := k + ":" + v

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

	tries := []merge.Merger{}

	var lastSegment *segment.Segment
	// keys from dimensions
	dimensionKeys := dimension.Intersection(dimensions...)
	for _, dk := range dimensionKeys {
		// TODO: refactor, store `Key`s in dimensions

		// parse the dimension key
		parsedKey, err := ParseKey(string(dk))
		if err != nil {
			return nil, fmt.Errorf("parse key: %v: %v", string(dk), err)
		}

		// get segment key
		sk := parsedKey.SegmentKey()
		// find segment from cache or badger
		se, err := s.getSegment(sk)
		if err != nil {
			return nil, fmt.Errorf("find segment %v: %v", sk, err)
		}
		// it's an aggregation
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

	return &GetOutput{
		Tree:       tr,
		Timeline:   timeline,
		SpyName:    lastSegment.SpyName(),
		SampleRate: lastSegment.SampleRate(),
		Units:      lastSegment.Units(),
	}, nil
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
			s.service.Del(tk)
			// delete the dict from cache and badger
			s.service.Del(FromTreeToMainKey(tk))
		})

		// delete the segment from cache and badger
		s.service.Del(key.SegmentKey())
	}

	// delete the dimensions from cache and badger
	for k, v := range di.Key.labels {
		s.service.Del(k + ":" + v)
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
