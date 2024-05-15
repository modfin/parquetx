package parquetx

import (
	"encoding/json"
	"fmt"
	"github.com/modfin/henry/slicez"
	s1 "github.com/xitongsys/parquet-go/schema"
	"reflect"
	"regexp"
	"strings"
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

var typemap = map[string]string{
	"bool":    "BOOLEAN",
	"byte":    "INT32",
	"int8":    "INT32",
	"int16":   "INT32",
	"int32":   "INT32",
	"int64":   "INT64",
	"int":     "INT64",
	"uint8":   "INT32",
	"uint16":  "INT32",
	"uint32":  "INT32",
	"uint64":  "INT64",
	"uint":    "INT64",
	"float32": "FLOAT",
	"float64": "DOUBLE",
	"string":  "BYTE_ARRAY",
}

var convtypes = map[string]string{
	"string": "UTF8",
	"int8":   "INT_8",
	"int16":  "INT_16",
	"int32":  "INT_32",
	"int64":  "INT_64",
	"int":    "INT_64",
	"byte":   "UINT_8",
	"uint8":  "UINT_8",
	"uint16": "UINT_16",
	"uint32": "UINT_32",
	"uint64": "UINT_64",
	"uint":   "UINT_64",
}

func NewJSONSchemaFromStruct(obj any) (str string, err error) {
	j := s1.NewJSONSchemaItem()
	j.Tag = "name=parquet_go_root, inname=Parquet_go_root repetitiontype=REQUIRED"

	var getFields func(obj reflect.Type) []*s1.JSONSchemaItemType

	var ensureRequire = func(tag *s1.JSONSchemaItemType) *s1.JSONSchemaItemType {
		if !strings.Contains(tag.Tag, "repetitiontype=") {
			tag.Tag = fmt.Sprintf("%s, repetitiontype=REQUIRED", tag.Tag)
		}
		return tag
	}

	getFields = func(ot reflect.Type) (tags []*s1.JSONSchemaItemType) {

		defer func() {
			tags = slicez.Map(tags, ensureRequire)
		}()

		next := func(f reflect.StructField) reflect.Type {
			t := f.Type
			if f.Type.Kind() == reflect.Ptr {
				t = f.Type.Elem()
			}
			return t
		}

		if ot.Kind() == reflect.Ptr { // add something about optional in tag?
			ot = ot.Elem()
		}

		if ot.Kind() == reflect.Map {

			keytype := typemap[ot.Key().String()]
			if len(keytype) > 0 {
				tag := &s1.JSONSchemaItemType{Tag: fmt.Sprintf("name=Key, type=%s, repetitiontype=REQUIRED", keytype)}
				tags = append(tags, tag)
			}
			valtype := typemap[ot.Elem().Name()]
			if len(valtype) == 0 {
				tag := &s1.JSONSchemaItemType{Tag: fmt.Sprintf("name=right, repetitiontype=REQUIRED")}
				tag.Fields = getFields(ot.Elem())
				tags = append(tags, tag)
			}
			if len(valtype) > 0 {
				tag := &s1.JSONSchemaItemType{Tag: fmt.Sprintf("name=right, type=%s, repetitiontype=REQUIRED", valtype)}
				tags = append(tags, tag)
			}

		}

		if ot.Kind() == reflect.Slice {

			switch ot.Elem().Kind() {
			case reflect.Slice, reflect.Struct, reflect.Map:
				return getFields(ot.Elem())
			}

		}
		if ot.Kind() == reflect.Struct {
			numField := int32(ot.NumField())
			for i := 0; i < int(numField); i++ {
				f := ot.Field(i)
				tagStr := f.Tag.Get("parquet")
				if tagStr == "-" {
					continue
				}
				if len(tagStr) > 0 { // todo ensure types
					t := &s1.JSONSchemaItemType{Tag: tagStr}
					t.Fields = getFields(next(f))
					tags = append(tags, t)
					continue
				}

				//ignore item without parquet tag
				parts := []string{
					fmt.Sprintf("name=%s", toSnakeCase(f.Name)),
					fmt.Sprintf("inname=%s", f.Name),
				}
				name := f.Type.Kind().String()
				_type := typemap[name]
				_convtype := convtypes[name]
				if name == "ptr" {
					name = f.Type.Elem().String()
					_type = typemap[name]
					_convtype = convtypes[name]
					parts = append(parts, "repetitiontype=OPTIONAL")
				}
				if name == "map" {
					parts = append(parts, "type=MAP")
					parts = append(parts, "repetitiontype=REQUIRED")
				}
				if name == "slice" && f.Type.Elem().Name() == "byte" {
					_type = "BYTE_ARRAY"
					_convtype = ""
				} else if name == "slice" {
					_type = typemap[f.Type.Elem().Name()]
					_convtype = convtypes[f.Type.Elem().Name()]
					parts = append(parts, "repetitiontype=REPEATED")
				}
				if len(_type) > 0 {
					parts = append(parts, fmt.Sprintf("type=%s", _type))
				}
				if len(_convtype) > 0 {
					parts = append(parts, fmt.Sprintf("convertedtype=%s", _convtype))
				}
				tagStr = strings.Join(parts, ", ")

				t := &s1.JSONSchemaItemType{Tag: tagStr}
				t.Fields = getFields(next(f))
				tags = append(tags, t)
			}
		}
		return tags
	}

	j.Fields = getFields(reflect.TypeOf(obj).Elem())

	b, err := json.MarshalIndent(j, "", "  ")
	return string(b), err

}
