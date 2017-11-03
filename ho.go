package ho

import (
	"database/sql"
	"fmt"
)

/*
几个db用的方法。
*/

type Values map[string]interface{}

// DB 数据操作简单封装
type DB interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Query 查询
func Query(conn DB, query string, args ...interface{}) ([]RowMap, error) {
	items := make([]RowMap, 0)
	rows, err := conn.Query(query, args...)
	if err != nil {
		return items, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return items, err
	}
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			_ = rows.Close()
			return items, err
		}
		items = append(items, RowMap{}.Scan(columns, values))
	}
	return items, nil
}

// QueryRow 查询单行
func QueryRow(conn DB, query string, args ...interface{}) (RowMap, error) {
	item := RowMap{}
	rows, err := conn.Query(query, args...)
	if err != nil {
		return item, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return item, err
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		return item.Scan(columns, values), nil
	}
	return item, nil
}

func QueryString(conn DB, query string, args ...interface{}) (string, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var val sql.RawBytes
	var valPoint = &val
	if rows.Next() {
		err = rows.Scan(valPoint)
		if err != nil {
			return "", err
		}
		return string(val), nil
	}
	return "", nil
}

// ExecSQL 执行
func ExecSQL(conn DB, query string, args ...interface{}) (sql.Result, error) {
	return conn.Exec(query, args...)
}

// ExecNon 执行
func ExecNon(conn DB, query string, args ...interface{}) error {
	_, err := ExecSQL(conn, query, args...)
	return err
}

// Exec 执行
func Exec(conn DB, query string, args ...interface{}) (int64, error) {
	r, err := ExecSQL(conn, query, args...)
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// InsertRaw 执行
func InsertRaw(conn DB, query string, args ...interface{}) (int64, error) {
	r, err := ExecSQL(conn, query, args...)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

// LimitSQL LimitSQL
func LimitSQL(offset, limit int64) string {
	if limit < 0 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}
	if offset == 0 {
		return fmt.Sprintf(" LIMIT %d", limit)
	}
	return fmt.Sprintf(" LIMIT %d, %d", offset, limit)
}

func Tx(conn *sql.DB, doSomething func(tx *sql.Tx) error) (err error) {
	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer func() {
		tx.Rollback()
		if ex := recover(); ex != nil {
			err = fmt.Errorf("%v", ex)
		}
	}()
	err = doSomething(tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}
