package ho

import (
	"fmt"
	"reflect"
	"strings"
)

var col_cache = make(map[string]columns)

type (
	Object interface {
		Tab() string
	}

	column struct {
		fieldTp string
		col     string
		field   string
		idx     bool
	}
	columns []column
)

func (cs *column) String() string {
	return fmt.Sprintf("fieldTp:%s, col:%s, field:%s, idx:%v", cs.fieldTp, cs.col, cs.field, cs.idx)
}

func (cs columns) Field(col string) string {
	for _, c := range cs {
		if c.col == col {
			return c.field
		}
	}
	return ""
}

func (cs columns) Col(field string) string {
	for _, c := range cs {
		if c.field == field {
			return c.col
		}
	}
	return ""
}

func getColumns(obj interface{}) columns {
	sn := getTp(obj)
	cols, ok := col_cache[sn]
	if !ok {
		tp := reflect.TypeOf(obj).Elem()
		cols = make([]column, 0)
		for i := 0; i < tp.NumField(); i++ {
			col := parseOTag(tp.Field(i))
			if col != nil {
				cols = append(cols, *col)
			}
		}
		col_cache[sn] = cols
	}
	return cols
}

func getTp(obj interface{}) string {
	tp := ""
	switch t := obj.(type) {
	default:
		tp = fmt.Sprintf("%v", t)
	}
	return tp
}

func parseOTag(field reflect.StructField) *column {
	tag, ok := field.Tag.Lookup("o")
	if !ok || tag == "-" {
		return nil
	}

	fn := field.Name
	col := &column{
		fieldTp: field.Type.Name(),
		field:   fn,
		col:     fn,
		idx:     false,
	}

	tags := strings.Split(tag, ",")
	col.idx = in_array(tags, "ai")
	if tags[0] != "" {
		col.col = tags[0]
	}
	return col
}

func in_array(array []string, value string) bool {
	for _, v := range array {
		if v == value {
			return true
		}
	}
	return false
}

func (row RowMap) Struct(obj interface{}) (find bool) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
			find = false
		}
	}()

	if row.IsEmpty() {
		return false
	}

	cols := getColumns(obj)
	objVal := reflect.ValueOf(obj).Elem()

	for _, col := range cols {
		if fVal := objVal.FieldByName(col.field); fVal.IsValid() {
			setVal(row, fVal, col.col)
		}
	}
	return true
}

func FindStruct(conn DB, obj Object, where string, params ...interface{}) (bool, error) {
	row, err := Find(conn, obj.Tab(), where, params...)
	if err != nil {
		return false, err
	}
	return row.Struct(obj), nil
}

func InsertStruct(conn DB, obj Object) error {
	cols := getColumns(obj)
	params := make(map[string]interface{})
	el := reflect.ValueOf(obj).Elem()
	var keyField reflect.Value
	for _, col := range cols {
		fieldValue := el.FieldByName(col.field)
		if !col.idx {
			params[col.col] = getVal(fieldValue, col.fieldTp)
		} else if !keyField.IsValid() {
			keyField = fieldValue
		}
	}

	id, err := Insert(conn, obj.Tab(), params)
	if err != nil {
		return err
	}

	if keyField.IsValid() {
		keyField.SetInt(id)
	}

	return nil
}

func UpdateStruct(conn DB, obj Object, where string, whereParams ...interface{}) error {
	return UpdateColumns(conn, obj, []string{}, where, whereParams...)
}

func UpdateColumns(conn DB, obj Object, update_cols []string, where string, whereParams ...interface{}) error {
	cols := getColumns(obj)
	params := make(map[string]interface{})
	el := reflect.ValueOf(obj).Elem()
	for _, col := range cols {
		ok := len(update_cols) == 0
		if !ok {
			ok = in_array(update_cols, col.col)
		}
		if !ok {
			ok = in_array(update_cols, col.field)
		}

		if ok {
			fieldValue := el.FieldByName(col.field)
			if fieldValue.IsValid() {
				if !col.idx {
					params[col.col] = getVal(fieldValue, col.fieldTp)
				}
			}
		}
	}

	_, err := Update(conn, obj.Tab(), params, where, whereParams...)
	return err
}

func getVal(fVal reflect.Value, fieldTp string) interface{} {
	switch fieldTp {
	case "int64", "int32", "int8", "int":
		return fVal.Int()
	case "uint64", "uint32", "uint8", "uint":
		return fVal.Uint()
	case "float64", "float32", "float8", "float":
		return fVal.Float()
	case "bool":
		return fVal.Bool()
	case "string":
		return fVal.String()
	case "[]byte":
		return fVal.Bytes()
	default:
		return nil
	}
}

func setVal(row RowMap, fVal reflect.Value, col string) {
	switch fVal.Type().String() {
	case "int64", "int32", "int8", "int":
		fVal.SetInt(row.Int(col))
	case "float64", "float32", "float8", "float":
		fVal.SetFloat(row.Float(col))
	case "string":
		fVal.SetString(row.String(col))
	case "bool":
		fVal.SetBool(row.Bool(col))
	}
}
