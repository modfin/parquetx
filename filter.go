package parquetx

import (
	"encoding/json"
	"fmt"
	"github.com/modfin/henry/compare"
	"github.com/modfin/henry/numz"
	"github.com/modfin/henry/slicez"
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

func PrettyPrint(data interface{}) {
	var p []byte
	//    var err := error
	p, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s \n", p)
}
func NewFilterReader(file source.ParquetFile) (*FilterReader, error) {
	r, err := reader.NewParquetColumnReader(file, int64(runtime.NumCPU()))
	//r, err := reader.NewParquetColumnReader(file, 1)
	if err != nil {
		return nil, err
	}
	defsize := int64(20_000)
	return &FilterReader{
		rows: int(r.Footer.GetNumRows()),
		size: int(r.Footer.GetNumRows()),
		file: file,
		col:  r,
		// Random heuristic that seems to work fine with n=1 testing.
		bufferSize: int(numz.Min(numz.Max(r.Footer.RowGroups[0].GetNumRows()/10, defsize), defsize)),
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

	bufferSize int
	buffer     []interface{}
}

type Operator string

var Equal Operator = "="
var NotEqual Operator = "<>"
var NotEqual2 Operator = "!="
var Less Operator = "<"
var LessEqual Operator = "<="
var Greater Operator = ">"
var GreaterEqual Operator = ">="
var In Operator = "in"
var Overlap Operator = "&&" // anyarray && anyarray → boolean => ARRAY[1,4,3] && ARRAY[2,1] → t
var Like Operator = "like"

type exprtype int

const column exprtype = 0
const value exprtype = 1

type Expr struct {
	_type exprtype
	Value any
	Col   string
}

func Col(name string) Expr {
	return Expr{
		_type: column,
		Col:   name,
	}
}
func Val(val any) Expr {
	return Expr{
		_type: value,
		Value: val,
	}
}

type filter struct {
	left  Expr
	op    Operator
	right Expr
}

func (f *filter) Eval(left any, right any) (include bool, err error) {
	left = upcast(left)
	right = upcast(right) // should really cache this
	switch f.op {
	case Equal:
		//fmt.Println(left, right, left == right, reflect.TypeOf(left), reflect.TypeOf(right))
		return left == right, nil
	case NotEqual, NotEqual2:
		return left != right, nil
	case Less:
		switch left.(type) {
		case int64:
			l := left.(int64)
			r, ok := right.(int64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a int64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l < r, nil
		case float64:
			l := left.(float64)
			r, ok := right.(float64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a float64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l < r, nil
		case string:
			l := left.(string)
			r, ok := right.(string)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a string and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l < r, nil

		}

	case Greater:
		switch left.(type) {
		case int64:
			l := left.(int64)
			r, ok := right.(int64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a int64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l > r, nil
		case float64:
			l := left.(float64)
			r, ok := right.(float64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a float64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l > r, nil
		case string:
			l := left.(string)
			r, ok := right.(string)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a string and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l > r, nil

		}
	case LessEqual:
		switch left.(type) {
		case int64:
			l := left.(int64)
			r, ok := right.(int64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a int64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l <= r, nil
		case float64:
			l := left.(float64)
			r, ok := right.(float64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a float64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l <= r, nil
		case string:
			l := left.(string)
			r, ok := right.(string)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a string and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l <= r, nil

		}
	case GreaterEqual:
		switch left.(type) {
		case int64:
			l := left.(int64)
			r, ok := right.(int64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a int64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l >= r, nil
		case float64:
			l := left.(float64)
			r, ok := right.(float64)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a float64 and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l >= r, nil
		case string:
			l := left.(string)
			r, ok := right.(string)
			if !ok {
				return false, fmt.Errorf("can not compare different types, '%s' is a string and right hand element, %v, is a %s", ".", right, reflect.TypeOf(right).String())
			}
			return l >= r, nil

		}
	case Overlap:
		var r interface{}
		var l interface{}

		switch left.(type) {
		case []interface{}, []int, []int64, []int32, []int16, []int8, []uint, []uint64, []uint32, []uint16, []uint8, []float64, []float32, []string:
			r = right
		default:
			return false, fmt.Errorf("right was not a slice, left=%s: %v  right=%s: %v", reflect.TypeOf(left), left, reflect.TypeOf(right), right)
		}
		switch left.(type) {
		case []interface{}, []int, []int64, []int32, []int16, []int8, []uint, []uint64, []uint32, []uint16, []uint8, []float64, []float32, []string:
			l = left
		default:
			return false, fmt.Errorf("right was not a slice, left=%s: %v  right=%s: %v", reflect.TypeOf(left), left, reflect.TypeOf(right), right)
		}

		set := map[interface{}]bool{}
		larr := reflect.ValueOf(l)
		rarr := reflect.ValueOf(r)

		if larr.Len() > 50 {
			for i := 0; i < larr.Len(); i++ {
				ll := upcast(larr.Index(i).Interface())
				set[ll] = true
			}
			for i := 0; i < rarr.Len(); i++ {
				rr := upcast(rarr.Index(i).Interface())
				if set[rr] {
					return true, nil
				}
			}
			return false, nil
		}

		if rarr.Len() > 50 {
			for i := 0; i < rarr.Len(); i++ {
				ll := upcast(rarr.Index(i).Interface())
				set[ll] = true
			}
			for i := 0; i < larr.Len(); i++ {
				ll := upcast(larr.Index(i).Interface())
				if set[ll] {
					return true, nil
				}
			}
			return false, nil
		}

		for i := 0; i < rarr.Len(); i++ {
			rr := upcast(rarr.Index(i).Interface())
			for i := 0; i < larr.Len(); i++ {
				ll := upcast(larr.Index(i).Interface())
				if ll == rr {
					return true, nil
				}
			}
		}
		return false, nil

	case In:

		var arr interface{}
		var val interface{}
		switch right.(type) {
		case []interface{}, []int, []int64, []int32, []int16, []int8, []uint, []uint64, []uint32, []uint16, []uint8, []float64, []float32, []string:
			arr = right
		default:
			return false, fmt.Errorf("right was not a slice, left=%s: %v  right=%s: %v", reflect.TypeOf(left), left, reflect.TypeOf(right), right)
		}
		switch left.(type) {
		//case []int, []int64, []int32, []int16, []int8, []uint, []uint64, []uint32, []uint16, []uint8, []float64, []float32, []string:
		//	arr = left
		case int64, float64, string:
			val = left
		case []interface{}:
			arr = left
		default:
			return false, fmt.Errorf("could not assign type for left, %s", reflect.TypeOf(left))
		}

		if arr == nil {
			return false, fmt.Errorf("no slice was provided, left=%s; %s, right=%s; %s", reflect.TypeOf(left), left, reflect.TypeOf(right), right)
		}
		if val == nil {
			return false, fmt.Errorf("no value was provided")
		}

		rarr := reflect.ValueOf(arr)
		for i := 0; i < rarr.Len(); i++ {
			part := rarr.Index(i).Interface()
			if upcast(part) == upcast(val) {
				return true, nil
			}
		}
		return false, nil

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

func (f *FilterReader) Filter(left Expr, op Operator, right Expr) {
	f.filters = append(f.filters, filter{
		left:  left,
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

	byPath := slicez.GroupBy(f.filters, func(f filter) string {
		return compare.Ternary(f.left._type == column, f.left.Col, f.right.Col)
	})

	for path, filters := range byPath {
		if path == "" { // it there are no columns in filter... ignore.
			continue
		}
		t = time.Now()
		pathStr, err := f.col.SchemaHandler.ConvertToInPathStr(common.ReformPathStr(fmt.Sprintf("parquet_go_root.%s", path)))
		if err != nil {
			return fmt.Errorf("could not find path string, %w", err)
		}

		buff, err := reader.NewColumnBuffer(f.file, f.col.Footer, f.col.SchemaHandler, pathStr)
		if err != nil {
			return fmt.Errorf("could not create buff, %w", err)
		}

		index, ok := buff.SchemaHandler.MapIndex[pathStr]
		if !ok {
			return fmt.Errorf("could not find schema for column")
		}
		repetitionLevel := buff.SchemaHandler.SchemaElements[index].GetRepetitionType()

		var idx []int
		var curr int
		for _, i := range f.idx {

			if i-curr > 0 {
				buff.SkipRows(int64(i - curr))
				curr = i
			}

			page, _ := buff.ReadRows(1) // TODO Implement bufferd version for speedup....
			var left any                // Probably left, but maybe not...
			left = page.Values[0]
			if repetitionLevel == parquet.FieldRepetitionType_REPEATED {
				left = page.Values
			}

			var include = true
			for _, fil := range filters {
				left := left
				if !include { // breaking early
					break
				}

				var right any

				if fil.left._type == value {
					right = fil.left.Value
				}
				if fil.right._type == value {
					right = fil.right.Value
				}
				if path == fil.right.Col {
					right, left = left, right
				}

				inc, err := fil.Eval(left, right)
				//fmt.Println("Eval, ", left, fil.op, right, "include", inc)
				if err != nil {
					return fmt.Errorf("could not eval, %w", err)
				}
				include = include && inc

			}

			if include {
				idx = append(idx, i)
			}

			curr = curr + 1
		}

		slog.Debug("[Apply] Filtering", "filters on", path, "duration", time.Since(t))
		t = time.Now()
		f.idx = intersection(f.idx, idx)
		slog.Debug("[Apply] intersection", "filter on", path, "duration", time.Since(t))

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
	f.buffer = nil
	return nil
}

// todo do something smart in regars to buffering result. Its more efficient to read all, then skip around at the moment.

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
	skip := int64(idx - f.cursor)

	if len(f.buffer) <= int(skip) {
		skip = skip - int64(len(f.buffer))
		f.buffer = nil
	}

	if len(f.buffer) == 0 {
		bufsize := numz.Min(f.bufferSize, f.rows-idx)
		//bufsize = numz.Min(f.bufferSize, f.rows-idx, 100)

		err := f.col.SkipRows(skip)
		if err != nil {
			return nil, fmt.Errorf("could not skip, %w", err)
		}

		f.buffer, err = f.col.ReadByNumber(bufsize)
		if err != nil {
			return nil, fmt.Errorf("could not read, %w", err)
		}
		skip = 0
	}

	f.buffer = f.buffer[skip:]

	f.cursor = idx + 1
	f.count = f.count + 1

	//if len(f.buffer) == 0 {
	//	return nil, io.EOF
	//}

	el := f.buffer[0]
	f.buffer = f.buffer[1:]
	return el, nil
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
