package cmd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dgraph-io/badger/v2"
	"github.com/spf13/cobra"

	"github.com/pyroscope-io/pyroscope/pkg/storage/dict"
	v2 "github.com/pyroscope-io/pyroscope/pkg/storage/tree"
	v1 "github.com/pyroscope-io/pyroscope/pkg/storage/tree/v1"
)

func (d *dbTool) newRevertCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "revert",
		RunE:    d.runRevert,
		PreRunE: d.openDB(false),
	}
}

func (d *dbTool) runRevert(_ *cobra.Command, _ []string) error {
	dicts, err := loadDictionaries()
	if err != nil {
		return fmt.Errorf("loading dictionaries: %w", err)
	}
	return d.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   1000,
		})
		defer it.Close()
		// "t:<key>:<depth>:<time>"
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			// Get dictionary.
			dictKey, ok := toDictKey(k)
			if !ok {
				fmt.Println(">>> Cannot get dict key from:", string(k))
				continue
			}
			dd, ok := dicts[dictKey]
			if !ok {
				fmt.Printf(">>> Cannot find dict %q [%s]\n", dictKey, string(k))
				continue
			}

			// Revert tree.
			var v []byte
			if v, err = item.ValueCopy(nil); err != nil {
				return fmt.Errorf("failed to copy value for key %q: %w", string(k), err)
			}
			var buf bytes.Buffer
			if ok, err = revertTree(dd, &buf, bytes.NewReader(v)); err != nil {
				return fmt.Errorf("failed to revert tree %q: %w", string(k), err)
			}
			if ok {
				if err = txn.Set(item.KeyCopy(nil), buf.Bytes()); err != nil {
					return fmt.Errorf("failed to store reverted tree %q: %w", string(k), err)
				}
				fmt.Println("> Reverted", string(k))
			}
		}
		return nil
	})
}

func toDictKey(b []byte) (string, bool) {
	if idx := bytes.IndexByte(b, '{'); idx > 0 {
		return string(b[2:idx]), true
	}
	return "", false
}

func loadDictionaries() (map[string]*dict.Dict, error) {
	db, err := openDB("dicts", true)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	dicts := map[string]*dict.Dict{}
	return dicts, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   1000,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			// "d:<key>"
			k := item.Key()
			err = item.Value(func(val []byte) error {
				if err != nil {
					return err
				}
				var d *dict.Dict
				if d, err = dict.Deserialize(bytes.NewReader(val)); err != nil {
					return fmt.Errorf("failed to deserialize dictionary %q: %w", string(k), err)
				}
				fmt.Println("Loading dictionary", string(k))
				dicts[string(k[2:])] = d
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func revertTree(d *dict.Dict, dst io.Writer, src io.Reader) (bool, error) {
	br := bufio.NewReader(src)
	version, err := binary.ReadUvarint(br)
	if err != nil {
		return false, err
	}
	var t2 *v2.Tree
	switch version {
	case 1:
		return false, nil
	case 2:
		if t2, err = v2.DeserializeV2(d, br); err != nil {
			return false, fmt.Errorf("v2.Deserialize: %w", err)
		}
	default:
		return false, fmt.Errorf("deserialize: unknown format version")
	}

	t1 := v1.New()
	t2.Iterate(func(key []byte, val uint64) {
		if len(key) > 2 && val != 0 {
			t1.Insert(key[2:], val)
		}
	})

	if err = t1.Serialize(d, 1<<14, dst); err != nil {
		return false, fmt.Errorf("v1.Serialize: %w", err)
	}

	return true, nil
}
