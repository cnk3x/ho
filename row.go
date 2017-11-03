package ho

import (
	"database/sql"
	"strconv"
)

// RowMap 数据行
type RowMap map[string]string

//IsEmpty 是否空的
func (m RowMap) IsEmpty() bool {
	return len(m) == 0
}

//String 返回string
func (m RowMap) String(name string) string {
	if val, ok := m[name]; ok {
		return val
	}
	return ""
}

//Int 返回int
func (m RowMap) Int(name string) int64 {
	if i, err := strconv.ParseInt(m.String(name), 10, 64); err == nil {
		return i
	}
	return 0
}

//Float ..
func (m RowMap) Float(name string) float64 {
	if i, err := strconv.ParseFloat(m.String(name), 64); err == nil {
		return i
	}
	return 0
}

//Bool 返回int
func (m RowMap) Bool(name string) bool {
	if i, err := strconv.ParseBool(m.String(name)); err == nil {
		return i
	}
	return false
}

//Scan .
func (m RowMap) Scan(keys []string, values []sql.RawBytes) RowMap {
	for i, val := range values {
		if val != nil && len(val) > 0 {
			m[keys[i]] = string(val)
		}
	}
	return m
}
