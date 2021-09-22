package tree

import (
	"bytes"
	"embed"
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"text/template"
)

var (
	//go:embed resources/*
	codegenResources embed.FS
)

type GoGenerator struct {
	functions []string
}

func toString(s []byte) string {
	return string(s)
}

func b64(s []byte) string {
	return base64.RawStdEncoding.EncodeToString(s)
}

func alphanum(b []byte) string {
	// TODO
	// replace slashes and periods for underscore?
	s := string(b)
	re := regexp.MustCompile("[^a-zA-Z0-9]+")
	t := re.ReplaceAllLiteralString(s, "")

	return t
}

func work(n uint64) string {
	return `for i := 0; i < ` + strconv.FormatUint(n, 10) + `; i++ {}`
}

func (t *Tree) GenerateGo() (string, error) {
	visited := make(map[string]int)

	r := `package main

import (
	"os"

	"github.com/pyroscope-io/pyroscope/pkg/agent/profiler"
)

func main() {
	pyroAddress := os.Getenv("PYROSCOPE_ADDRESS")

	if pyroAddress != "" {
		profiler.Start(profiler.Config{
			ApplicationName: "codegen.golang.app",
			ServerAddress:   "http://localhost:4040",
		})
	}

	for {
		main0()
	}
}
`

	var s queue
	s.Push(t.root)
	t.root.Name = []byte("main")

	for !s.Empty() {
		curr := s.Pop()

		name := alphanum(curr.Name)

		// already exists
		if _, ok := visited[name]; ok {
			visited[name]++
		} else {
			// initializes to 0
			visited[name] = 0
		}

		for _, v := range curr.ChildrenNodes {
			s.Push(v)
		}

		var tpl bytes.Buffer
		t, err := template.New("go.gotpl").Funcs(template.FuncMap{
			"b64":       b64,
			"string":    toString,
			"normalize": alphanum,
			"work":      work,
		}).ParseFS(codegenResources, "resources/go.gotpl")

		if err != nil {
			return "", err
		}

		cn := make([]string, len(curr.ChildrenNodes))
		for i, v := range curr.ChildrenNodes {
			n := alphanum(v.Name)
			index := 0

			// already exists
			if _, ok := visited[n]; ok {
				index = visited[n] + 1
			} else {
				index = 0
			}

			cn[i] = fmt.Sprintf("%s%d", n, index)
		}

		data := struct {
			Name          string
			ChildrenNodes []string
			Work          uint64
		}{
			Name:          fmt.Sprintf("%s%d", name, visited[name]),
			ChildrenNodes: cn,
			Work:          curr.Self,
		}

		err = t.Execute(&tpl, data)
		if err != nil {
			return "", err
		}

		r = r + tpl.String()
	}

	return string(r), nil
}

type queue []*treeNode

func (q *queue) Push(t *treeNode) {
	*q = append(*q, t)
}

func (q *queue) Pop() *treeNode {
	if q.Empty() {
		panic("popping from an empty stack")
	}

	el := (*q)[0]

	(*q)[0] = nil
	*q = (*q)[1:]

	return el
}

func (q *queue) Empty() bool {
	return len(*q) <= 0
}
