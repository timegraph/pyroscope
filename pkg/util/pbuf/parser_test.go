package pbuf

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pyroscope-io/pyroscope/pkg/agent/spy"
	"github.com/pyroscope-io/pyroscope/pkg/convert"
)

func parserOutputToString(parser PprofParser) string {
	res := ""
	parser.Get("samples", func(labels *spy.Labels, name []byte, val int) {
		res += fmt.Sprintf("<%s> %q %d\n", labels.ID(), name, val)
	})
	log.Println("---")
	log.Println(res)
	return res
}

var _ = Describe("pbuf package", func() {
	Describe("Parser", func() {
		It("parser pprof files", func() {
			b, err := ioutil.ReadFile("../../convert/testdata/cpu.pprof")
			Expect(err).ToNot(HaveOccurred())
			r := bytes.NewReader(b)
			g, err := gzip.NewReader(r)
			Expect(err).ToNot(HaveOccurred())

			b, err = ioutil.ReadAll(g)
			Expect(err).ToNot(HaveOccurred())

			p := New(b)

			p.Get("samples", func(labels *spy.Labels, name []byte, val int) {
				// TODO
			})
		})

		It("is an invariant of protobuf parser", func() {
			b, err := ioutil.ReadFile("../../convert/testdata/cpu.pprof")
			Expect(err).ToNot(HaveOccurred())
			r := bytes.NewReader(b)
			g, err := gzip.NewReader(r)
			Expect(err).ToNot(HaveOccurred())
			// p, err := ParsePprof(g)

			pprofBytes, err := ioutil.ReadAll(g)
			Expect(err).ToNot(HaveOccurred())

			customParser := New(pprofBytes)
			protobufParser, err := convert.ParsePprof(bytes.NewReader(pprofBytes))
			// b2, _ := json.MarshalIndent(protobufParser, "", "  ")
			// fmt.Println(string(b2))
			Expect(err).ToNot(HaveOccurred())

			Expect(parserOutputToString(customParser)).To(Equal(parserOutputToString(protobufParser)))
		})

		Context("standard parser", func() {
			Measure("it should parse efficiently", func(bench Benchmarker) {
				b, err := ioutil.ReadFile("../../convert/testdata/cpu.pprof")
				Expect(err).ToNot(HaveOccurred())
				r := bytes.NewReader(b)
				g, err := gzip.NewReader(r)
				Expect(err).ToNot(HaveOccurred())
				pprofBytes, err := ioutil.ReadAll(g)
				Expect(err).ToNot(HaveOccurred())
				n := 100

				runtime := bench.Time("runtime", func() {
					for i := 0; i < n; i++ {
						customParser := New(pprofBytes)
						parserOutputToString(customParser)
					}
				})

				runtimeOriginal := bench.Time("runtime", func() {
					for i := 0; i < n; i++ {
						protobufParser, _ := convert.ParsePprof(bytes.NewReader(pprofBytes))
						parserOutputToString(protobufParser)
					}
				})

				log.Println("runtime custom", runtime.Seconds())
				log.Println("runtime original", runtimeOriginal.Seconds())

				Expect(runtime.Seconds()).To(BeNumerically("<", runtimeOriginal.Seconds()), "custom implementation has to be faster")
			}, 10)
		})
	})
})
