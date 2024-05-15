package parquetx

import (
	"fmt"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/tool/parquet-tools/schematool"
	"io"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"time"
)

func NewFilterReader(file source.ParquetFile) (*FilterReader, error) {
	r, err := reader.NewParquetColumnReader(file, int64(runtime.NumCPU()))
	//r, err := reader.NewParquetColumnReader(file, 1)
	if err != nil {
		return nil, err
	}
	l := r.GetNumRows()
	return &FilterReader{
		rows: int(l),
		size: int(l),
		file: file,
		col:  r,
	}, nil
}

type FilterReader struct {
	rows int
	size int

	col     *reader.ParquetReader
	file    source.ParquetFile
	filters []filter
	idx     []int

	cursor int
	count  int
}

type Operator string

var Equal Operator = "=="
var NotEqual Operator = "!="
var Less Operator = "<"
var LessEqual Operator = "<="
var Greater Operator = ">"
var GreaterEqual Operator = ">="
var In Operator = "in"
var Like Operator = "like"

type filter struct {
	path  string
	op    Operator
	right any
}

func (f *filter) Eval(left any) (include bool, err error) {
	left = upcast(left)
	right := upcast(f.right)
	switch f.op {
	case Equal:
		return left == right, nil
	case Less:
		switch left.(type) {
		case int64:
			l := left.(int64)
			r, ok := right.(int64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a int64 and right hand element, %v, is a %s", f.path, right, reflect.TypeOf(right).String())
			}
			return l < r, nil
		case float64:
			l := left.(float64)
			r, ok := right.(float64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a float64 and right hand element, %v, is a %s", f.path, right, reflect.TypeOf(right).String())
			}
			return l < r, nil
		case string:
			l := left.(string)
			r, ok := right.(string)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a string and right hand element, %v, is a %s", f.path, right, reflect.TypeOf(right).String())
			}
			return l < r, nil

		}

	case Greater:
	case LessEqual:
	case GreaterEqual:
	case In:
	case Like:
		l, ok := left.(string)
		if !ok {
			return false, fmt.Errorf("left is not a string, left: %v", l)
		}
		r, ok := right.(string)
		if !ok {
			return false, fmt.Errorf("right is not a string, right: %v", l)
		}
		if r[0] == '%' {
			return strings.HasSuffix(l, r[1:]), nil
		}
		if r[len(r)-1] == '%' {
			return strings.HasPrefix(l, r[:len(r)-1]), nil
		}

	}
	return false, fmt.Errorf("could not find op for %s", f.op)
}

func (f *FilterReader) Filter(path string, op Operator, right any) {
	f.filters = append(f.filters, filter{
		path:  path,
		op:    op,
		right: right,
	})
}
func (f *FilterReader) Len() int {
	return int(f.rows)
}
func (f *FilterReader) FilteredLen() int {
	return f.size
}

func upcast(e any) any {
	switch i := e.(type) {
	case int:
		return int64(i)
	case int8:
		return int64(i)
	case int16:
		return int64(i)
	case int32:
		return int64(i)
	case int64:
		return i
	case uint:
		return int64(i)
	case uint8:
		return int64(i)
	case uint16:
		return int64(i)
	case uint32:
		return int64(i)
	case uint64:
		return int64(i)
	case float32:
		return float64(i)
	case float64:
		return i
	}
	return e
}

func (f *FilterReader) Apply() (err error) {
	at := time.Now()
	defer func() {
		slog.Debug("[Apply] full", "duration", time.Since(at))
	}()

	defer func() {
		if err == nil {
			err = f.Reset()
		}
	}()

	t := time.Now()
	f.idx = make([]int, f.rows)
	for i := range f.idx {
		f.idx[i] = i
	}
	slog.Debug("[Apply] creating base", "duration", time.Since(t))

	for filterNum, fil := range f.filters {
		t = time.Now()

		pathStr, err := f.col.SchemaHandler.ConvertToInPathStr(common.ReformPathStr(fmt.Sprintf("parquet_go_root.%s", fil.path)))
		if err != nil {
			return fmt.Errorf("could not find path string, %w", err)
		}

		buff, err := reader.NewColumnBuffer(f.file, f.col.Footer, f.col.SchemaHandler, pathStr)
		if err != nil {
			return fmt.Errorf("could not create buff, %w", err)
		}

		var idx []int
		var curr int
		for _, i := range f.idx {

			if i-curr > 0 {
				buff.SkipRows(int64(i - curr))
				curr = i
			}

			page, _ := buff.ReadRows(1)
			v := page.Values[0]
			include, err := fil.Eval(v)
			if err != nil {
				return fmt.Errorf("could not eval, %w", err)
			}

			if include {
				idx = append(idx, i)
			}

			curr = curr + 1
		}

		slog.Debug("[Apply] Filtering", "filter", filterNum, "duration", time.Since(t))
		t = time.Now()
		f.idx = intersection(f.idx, idx)
		slog.Debug("[Apply] intersection", "filter", filterNum, "duration", time.Since(t))

	}
	f.size = len(f.idx)
	return nil

}

func intersection(aa, bb []int) []int {
	var res []int

	start := 0
	for i := 0; i < len(aa); i++ {
		a := aa[i]
		for j := start; j < len(bb); j++ {
			b := bb[j]
			if a == b {
				res = append(res, a)
				start = j + 1
				break
			}
			if a < b {
				start = j
				break
			}
		}
	}
	return res
}
func (f *FilterReader) Reset() error {
	t := time.Now()
	defer func() {
		slog.Debug("Reset", "duration", time.Since(t))
	}()
	// Resetting buffers...
	f.col.ColumnBuffers = map[string]*reader.ColumnBufferType{}
	var err error
	for _, pathStr := range f.col.SchemaHandler.ValueColumns {
		if _, ok := f.col.ColumnBuffers[pathStr]; !ok {
			if f.col.ColumnBuffers[pathStr], err = reader.NewColumnBuffer(f.col.PFile, f.col.Footer, f.col.SchemaHandler, pathStr); err != nil {
				return err
			}
		}
	}
	f.cursor = 0
	f.count = 0
	return nil
}

func (f *FilterReader) ReadAll() ([]interface{}, error) {
	t := time.Now()
	defer func() {
		slog.Debug("[ReadAll]", "duration", time.Since(t))
	}()

	err := f.Reset()
	if err != nil {
		return nil, err
	}
	var res []interface{}
	for {
		e, err := f.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		res = append(res, e)
	}
	return res, nil
}

func (f *FilterReader) Next() (any, error) {

	if len(f.idx) == 0 || len(f.idx) <= f.count {
		return nil, io.EOF
	}

	idx := f.idx[f.count]

	err := f.col.SkipRows(int64(idx - f.cursor))
	f.cursor = idx + 1
	f.count = f.count + 1
	if err != nil {
		return nil, fmt.Errorf("could not skip, %w", err)
	}

	rr, err := f.col.ReadByNumber(1)

	if err != nil {
		return nil, err
	}

	return rr[0], nil
}

func (f *FilterReader) AllBrute() ([]any, error) {
	t := time.Now()
	defer func() {
		slog.Debug("[AllBrute]", "duration", time.Since(t))
	}()
	err := f.Reset()
	if err != nil {
		return nil, err
	}

	tt := time.Now()
	rr, err := f.col.ReadByNumber(int(f.col.GetNumRows()))
	slog.Debug("[AllBrute] marshaling", "duration", time.Since(tt))
	tt = time.Now()

	if err != nil {
		return nil, err
	}
	var res []interface{}

	for _, i := range f.idx {
		res = append(res, rr[i])
	}
	slog.Debug("[AllBrute] filtering", "duration", time.Since(tt))

	return res, nil
}

func PrintSchema(file source.ParquetFile) error {
	r, err := reader.NewParquetColumnReader(file, 1)
	if err != nil {
		return err
	}
	Print(r.Footer.Schema)

	return err
}

func Print(schema []*parquet.SchemaElement) {
	tree := schematool.CreateSchemaTree(schema)

	var printnode func(*schematool.Node, int)
	printnode = func(n *schematool.Node, i int) {
		if n == nil {
			return
		}
		fmt.Println(strings.Repeat("  ", i), n.SE.Name, fmt.Sprintf("[%s; %s; %s]", n.SE.Type, n.SE.ConvertedType, n.SE.RepetitionType))
		for _, c := range n.Children {
			printnode(c, i+1)
		}

	}

	printnode(tree.Root, 0)

	//fmt.Printf(">> %+v\n", tree.OutputJsonSchema())
	//for _, s := range schema {
	//
	//	fmt.Println(s.Name, fmt.Sprintf("[%s]", s.Type))
	//	fmt.Printf(">> %+v\n", s)
	//}
}
