package pbuf

import (
	"bytes"
	"encoding/binary"

	"github.com/pyroscope-io/pyroscope/pkg/agent/spy"
	"github.com/valyala/bytebufferpool"
)

const (
	WireVarint          uint64 = 0
	WireFixed64                = 1
	WireLengthDelimited        = 2
	WireStartGroup             = 3 // deprecated
	WireEndGroup               = 4 // deprecated
	WireFixed32                = 5
)

type Node struct {
	w uint64
	t uint64
	b []byte
	o int
}

func New(b []byte) *Node {
	return &Node{
		w: WireLengthDelimited,
		t: 0,
		b: b,
	}
}

// func (p *Node) Read(args []int, cb func()) {

// }

func (p *Node) ReadVarint() (uint64, bool) {
	var x uint64
	var s uint
	l := len(p.b)
	for i := 0; i < 10; i++ {
		if p.o >= l {
			return 0, false
		}
		b := p.b[p.o]
		p.o++
		if b < 0x80 {
			if i == 10-1 && b > 1 {
				return x, false
			}
			return x | uint64(b)<<s, true
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return x, false
}

func (p *Node) ReadKey() (uint64, uint64, bool) {
	a, ok := p.ReadVarint()
	if ok {
		return a >> 3, a & 7, true
	}
	// tag, wiretype
	return 0, 0, false
}

func (p *Node) ReadNode() (*Node, bool) {
	tag, wireType, ok := p.ReadKey()
	if !ok {
		return nil, false
	}

	oldO := p.o

	// log.Println("wireType", wireType)
	switch wireType {
	case WireVarint:
		_, ok := p.ReadVarint() // TODO: implement a skip function
		if !ok {
			return nil, false
		}
	case WireFixed64:
		p.o += 8 // TODO: read value?
		if p.o >= len(p.b) {
			return nil, false
		}
	case WireFixed32:
		p.o += 4 // TODO: read value?
		if p.o >= len(p.b) {
			return nil, false
		}
	case WireLengthDelimited:
		l, ok := p.ReadVarint()
		oldO = p.o
		if !ok {
			return nil, false
		}
		p.o += int(l) // TODO: read value?
		if p.o > len(p.b) {
			return nil, false
		}
	}

	return &Node{
		w: wireType,
		t: tag,
		b: p.b[oldO:p.o],
	}, true
}

// func (p *Node) ReadLengthDelimited() []byte {
// 	return p.b
// }

// func (p *Node) ReadInt64() int64 {
// 	return 0
// }

// func (p *Node) ReadUint64() uint64 {
// 	return 0
// }

func (p *Node) ToString() string {
	return string(p.b)
}
func (p *Node) ToUint64() uint64 {
	return binary.BigEndian.Uint64(p.b)
}
func (p *Node) ToVarint() uint64 {
	v, _ := p.ReadVarint()
	return v
}
func (p *Node) ToVarintPacked() []uint64 {
	r := []uint64{}
	for {
		v, ok := p.ReadVarint()
		if !ok {
			break
		}
		r = append(r, v)
	}
	return r
}

func (p *Node) Each(cb func(*Node)) {
	p.o = 0
	for {
		n, ok := p.ReadNode()
		if !ok {
			break
		}
		cb(n)
	}
}

type cacheKey []int64

type cacheEntry struct {
	key []int
	val *spy.Labels
}
type cache struct {
	data []*cacheEntry
}

func newCache() *cache {
	return &cache{
		data: []*cacheEntry{},
	}
}

// func getCacheKey(l []*Label) cacheKey {
// 	r := []int64{}
// 	for _, x := range l {
// 		if x.Str != 0 {
// 			r = append(r, x.Key, x.Str)
// 		}
// 	}
// 	return r
// }

func eq(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func (c *cache) pprofLabelsToSpyLabels(stringTable [][]byte, labelsIntArr []int) *spy.Labels {
	k := labelsIntArr
	for _, e := range c.data {
		if eq(e.key, k) {
			return e.val
		}
	}

	l := spy.NewLabels()
	for i := 0; i < len(labelsIntArr); i += 2 {
		keyIndex := labelsIntArr[i]
		strIndex := labelsIntArr[i+1]
		if strIndex != 0 {
			l.Set(string(stringTable[keyIndex]), string(stringTable[strIndex]))
		}
	}

	newVal := &cacheEntry{
		key: k,
		val: l,
	}
	c.data = append(c.data, newVal)
	return l
}

type PprofParser interface {
	Get(sampleType string, cb func(labels *spy.Labels, name []byte, val int)) error
}

func (x *Node) Get(sampleType string, cb func(labels *spy.Labels, name []byte, val int)) error {
	sampleTypes := []uint64{}
	stringTable := [][]byte{}
	locationToFunctionIDS := map[int][]int{}
	functionToStringIndex := map[int]int{}
	x.Each(func(n *Node) {
		if n.t == 1 { // sample_type
			n.Each(func(valueTypeChild *Node) {
				if valueTypeChild.t == 1 { // sample_type
					sampleTypes = append(sampleTypes, valueTypeChild.ToVarint())
				}
			})
		} else if n.t == 6 { // string_table
			stringTable = append(stringTable, n.b)
		} else if n.t == 4 { // location
			lid := 0
			functionIDS := []int{}
			n.Each(func(locationChild *Node) {
				if locationChild.t == 1 { // location_id
					lid = int(locationChild.ToVarint())
				} else if locationChild.t == 4 { // line (repeated)
					locationChild.Each(func(lineChild *Node) {
						if lineChild.t == 1 { // function_id
							functionID := int(lineChild.ToVarint())
							functionIDS = append([]int{functionID}, functionIDS...)
						}
					})
				}
			})
			// if _, ok := locationToFunctionIDS[lid]; !ok {
			locationToFunctionIDS[lid] = functionIDS
			// }
		} else if n.t == 5 { // function
			fid := 0
			nameIndex := 0
			n.Each(func(functionChild *Node) {
				if functionChild.t == 1 {
					fid = int(functionChild.ToVarint())
				} else if functionChild.t == 2 {
					nameIndex = int(functionChild.ToVarint())
				}
			})
			functionToStringIndex[fid] = nameIndex
		}
	})

	selectedValueIndex := 0
	if sampleType != "" {
		for i, v := range sampleTypes {
			if bytes.Equal(stringTable[v], []byte(sampleType)) {
				selectedValueIndex = i
				break
			}
		}
	}

	x.Each(func(n *Node) {
		if n.t == 6 { // string table
			stringTable = append(stringTable, n.b)
		}
	})

	labelsCache := newCache()

	// TODO: not sure if this should be here or somewhere else
	b := bytebufferpool.Get()
	defer bytebufferpool.Put(b)

	x.Each(func(n *Node) {
		if n.t == 2 { // sample

			value := 0
			valueIndex := 0

			labelsIntArr := []int{}
			locationIDS := []uint64{}
			n.Each(func(sampleChild *Node) {
				if sampleChild.t == 1 { // location_id
					if sampleChild.w == WireLengthDelimited {
						ints := sampleChild.ToVarintPacked()
						locationIDS = append(locationIDS, ints...)
					} else { // TODO: this should be unified with previous
						locationID := sampleChild.ToVarint()
						locationIDS = append(locationIDS, locationID)
					}
				} else if sampleChild.t == 2 { // value
					if sampleChild.w == WireLengthDelimited {
						ints := sampleChild.ToVarintPacked()
						value = int(ints[selectedValueIndex])
					} else { // TODO: this should be unified with previous
						if selectedValueIndex == valueIndex {
							value = int(sampleChild.ToVarint())
						}
						valueIndex++
					}
				} else if sampleChild.t == 3 { // label
					keyIndex := 0
					strIndex := 0
					sampleChild.Each(func(labelChild *Node) {
						if labelChild.t == 1 { // key
							keyIndex = int(labelChild.ToVarint())
						} else if labelChild.t == 2 { // str
							strIndex = int(labelChild.ToVarint())
						}
					})
					labelsIntArr = append(labelsIntArr, keyIndex, strIndex)
				}
			})

			for i := len(locationIDS) - 1; i >= 0; i-- {
				locationID := int(locationIDS[i])
				functionIDS := locationToFunctionIDS[locationID]
				for _, functionID := range functionIDS {
					nameIndex := functionToStringIndex[functionID]
					nameBytes := stringTable[nameIndex]
					if b.Len() > 0 {
						_ = b.WriteByte(';')
					}
					_, _ = b.Write(nameBytes)
				}
			}

			labels := labelsCache.pprofLabelsToSpyLabels(stringTable, labelsIntArr)
			cb(labels, b.Bytes(), value)
			b.Reset()
		}
	})

	return nil
}
