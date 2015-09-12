package dbx

import (
	"database/sql/driver"
	"errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	ErrProtocol = errors.New("unrecognized protocol")
)

func Open(url string) (db *sqlx.DB, err error) {
	switch {
	case strings.HasPrefix(url, "mysql://"):
		db, err = sqlx.Open(`mysql`, url[len("mysql://"):])
	case strings.HasPrefix(url, "postgres://"):
		db, err = sqlx.Open(`postgres`, url)
	case strings.HasPrefix(url, "mssql://"):
		db, err = sqlx.Open("mssql", splitMssql(url))
	default:
		db, err = nil, ErrProtocol
	}
	return db, err
}

func splitMssql(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	user := u.User.Username()
	pass, _ := u.User.Password()
	return fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s", u.Host, user, pass, strings.Trim(u.Path, "/"))
}

// NullTime represents a time.Time that may be null. NullTime implements the
// sql.Scanner interface so it can be used as a scan destination, similar to
// sql.NullString.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL

}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil

}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil

	}
	return nt.Time, nil

}

type StringMapper struct {
	mu    sync.Mutex
	db    *sqlx.DB
	arc   map[interface{}]string
	query string
}

func MapString(db *sqlx.DB, query string) *StringMapper {
	return &StringMapper{
		db:    db,
		arc:   make(map[interface{}]string),
		query: query,
	}
}

func (m *StringMapper) Map(key interface{}) (val string, err error) {
	m.mu.Lock()
	x, ok := m.arc[key]
	m.mu.Unlock()
	if ok {
		return x, nil
	}
	row := m.db.QueryRow(m.query, key)
	if err = row.Scan(&val); err == nil {
		m.mu.Lock()
		m.arc[key] = val
		m.mu.Unlock()
	}
	return val, err
}

func (m *StringMapper) MustMap(key interface{}) string {
	val, err := m.Map(key)
	if err != nil {
		panic(err)
	}
	return val
}

// func ProtoWrap(dst interface{}, src interface{}) error {
// 	dv := reflect.ValueOf(dst)
// 	sv := reflect.ValueOf(src)
// 	numField := dv.NumField()
// 	if numField != sv.NumField() {
// 		panic(errors.New(fmt.Sprint("dst/src field mismatch")))
// 	}
// 	for i := 0; i < numField; i++ {
// 		sf := sv.Field(i)
// 		dv := dv.Field(i)
// 		switch sf.Interface().(type) {
// 		default:
// 			panic("unsupported field type")
// 		case bool:
// 			p := (*bool)(unsafe.Pointer(sf.Pointer()))
// 			dv.SetPointer(reflect.ValueOf(p))
// 		case int:
// 			p := (*int)(unsafe.Pointer(sf.Pointer()))
// 			dv.SetPointer(reflect.ValueOf(p))
// 		case string:
// 			p := (*string)(unsafe.Pointer(sf.Pointer()))
// 			dv.SetPointer(reflect.ValueOf(p))
// 		case time.Time:
// 			x := sv.Interface().(time.Time)
// 			s, n := x.Unix(), int32(x.Nanosecond())
// 			p := &Timestamp{
// 				Seconds: &s,
// 				Nanos:   &n,
// 			}
// 			dv.SetPointer(reflect.ValueOf(p))
// 		}
// 	}
// }
