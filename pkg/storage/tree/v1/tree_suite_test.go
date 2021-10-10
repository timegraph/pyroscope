package v1

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pyroscope-io/pyroscope/pkg/storage/dict"
	v2 "github.com/pyroscope-io/pyroscope/pkg/storage/tree/v2"
	testing2 "github.com/pyroscope-io/pyroscope/pkg/testing"
)

func TestTree(t *testing.T) {
	testing2.SetupLogging()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Tree Suite")
}

// v2
func TestX(t *testing.T) {
	x := New()
	x.Insert([]byte("foo;bar;baz"), 1)
	x.Insert([]byte("foo;bar"), 1)
	x.Insert([]byte("foo;bar2"), 1)
	x.Insert([]byte("foo"), 1)
	x.Insert([]byte("abc;def"), 1)

	// Create v2 binary fixture.
	d := dict.New()
	f, err := os.Create("testdata/tree.v2.bin")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	var b bytes.Buffer
	w := io.MultiWriter(&b, f)
	if err = x.Serialize(d, 1000, w); err != nil {
		panic(err)
	}

	// Create v2 text fixture.
	d2 := dict.New()
	t2, err := Deserialize(d2, bufio.NewReader(&b))
	if err != nil {
		panic(err)
	}
	f2, err := os.Create("testdata/tree.v2.txt")
	if err != nil {
		panic(err)
	}
	defer f2.Close()
	t2.Iterate(func(key []byte, val uint64) {
		_, _ = fmt.Fprintln(f2, string(key), val)
	})
}

func TestConvert(t *testing.T) {
	// Open v2.
	d := dict.New()
	f, err := os.Open("testdata/tree.v2.bin")
	if err != nil {
		panic(err)
	}
	x, err := v2.Deserialize(d, f)
	if err != nil {
		panic(err)
	}
	f.Close()
	// Save as v1.
	d2 := dict.New()
	f2, err := os.Create("testdata/tree.v1.bin")
	if err != nil {
		panic(err)
	}

	z := New()
	x.Iterate(func(key []byte, val uint64) {
		if len(key) > 2 && val != 0 {
			fmt.Println(string(key[2:]), val)
			z.Insert(key[2:], val)
		}
	})
	if err = z.Serialize(d2, 10000, f2); err != nil {
		panic(err)
	}
	f2.Close()
}

func TestY(t *testing.T) {
	// Check.
	f, err := os.Open("testdata/tree.v1.bin")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	d := dict.New()
	x, err := Deserialize(d, f)
	if err != nil {
		panic(err)
	}

	var actual bytes.Buffer
	x.Iterate(func(key []byte, val uint64) {
		_, _ = fmt.Fprintln(&actual, string(key), val)
		fmt.Println(string(key), val)
	})

	expected, err := os.ReadFile("testdata/tree.v2.txt")
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(actual.Bytes(), expected) {
		t.Fatalf("nope")
	}
}
