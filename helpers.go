package ho

import (
	"fmt"
	"strings"
)

func Insert(conn DB, table string, value_params Values) (id int64, err error) {
	names := make([]string, 0)
	values := make([]interface{}, 0)
	for name, value := range value_params {
		names = append(names, name)
		values = append(values, value)
	}
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(names, ", "),
		strings.Repeat(", ?", len(names))[1:],
	)
	return InsertRaw(conn, q, values...)
}

func Update(conn DB, table string, value_params Values, where string, whereParams ...interface{}) (rc int64, err error) {
	names := make([]string, 0)
	values := make([]interface{}, 0)
	for name, value := range value_params {
		names = append(names, fmt.Sprintf("%s=?", name))
		values = append(values, value)
	}
	q := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		table,
		strings.Join(names, ", "),
		where,
	)
	values = append(values, whereParams...)
	return Exec(conn, q, values...)
}

func Find(conn DB, table string, where string, params ...interface{}) (RowMap, error) {
	return QueryRow(conn, fmt.Sprintf("select * from %s where %s", table, where), params...)
}

func FindAll(conn DB, table string, offset, limit int64, where string, params ...interface{}) ([]RowMap, error) {
	if len(where) > 0 {
		where = " where " + where
	}

	return Query(conn, fmt.Sprintf("select * from %s%s%s",
		table,
		where,
		LimitSQL(offset, limit)),
		params...)
}

func Exist(conn DB, table string, where string, params ...interface{}) (bool, error) {
	r, err := Find(conn, table, where, params...)
	return !r.IsEmpty(), err
}

func Delete(conn DB, table string, where string, params ...interface{}) (int64, error) {
	return Exec(conn, fmt.Sprintf("delete from %s where %s", table, where), params...)
}
