package dimension

import (
	"bufio"
	"bytes"
	"io"

	"github.com/pyroscope-io/pyroscope/pkg/util/varint"
)

// serialization format version. it's not very useful right now, but it will be in the future
const currentVersion = 1

func (d *Dimension) Serialize(w io.Writer) error {
	varint.Write(w, currentVersion)

	for _, k := range d.keys {
		varint.Write(w, uint64(len(k)))
		w.Write([]byte(k))
	}
	return nil
}

// deserialize the bytes to Dimension
func deserialize(r io.Reader) (*Dimension, error) {
	nd := New()

	br := bufio.NewReader(r) // TODO if it's already a bytereader skip

	// reads serialization format version, see comment at the top
	_, err := varint.Read(br)
	if err != nil {
		return nil, err
	}

	for {
		keyLen, err := varint.Read(br)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		keyBuf := make([]byte, keyLen) // TODO: there are better ways to do this?
		_, err = io.ReadAtLeast(br, keyBuf, int(keyLen))
		if err != nil {
			return nil, err
		}

		nd.keys = append(nd.keys, Key(keyBuf))
	}

	return nd, nil
}

func (d *Dimension) Bytes() ([]byte, error) {
	b := bytes.Buffer{}
	if err := d.Serialize(&b); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func FromBytes(p []byte) (*Dimension, error) {
	return deserialize(bytes.NewReader(p))
}
