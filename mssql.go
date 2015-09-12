package dbx

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

// mssql information schema

type MSSQLSchema struct {
	Catalog string `db:"TABLE_CATALOG"`
	Schema  string `db:"TABLE_SCHEMA"`
	Name    string `db:"TABLE_NAME"`
	Type    string `db:"TABLE_TYPE"`
}

func Tables(db *sqlx.DB) (r []MSSQLSchema, err error) {
	err = db.Select(&r, `SELECT * FROM INFORMATION_SCHEMA.TABLES`)
	return r, err
}

type MSSQLColumn struct {
	Name     string `db:"COLUMN_NAME"`
	Type     string `db:"DATA_TYPE"`
	Nullable string `db:"IS_NULLABLE"`
}

func Columns(db *sqlx.DB, table string) (r []MSSQLColumn, err error) {
	err = db.Select(&r, fmt.Sprintf(`SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'`, table))
	return r, err
}
