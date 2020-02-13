package cbdb

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jinzhu/gorm"
)

type GormReadWrite struct {
	read      *gorm.DB
	write     *gorm.DB
	readmock  sqlmock.Sqlmock
	writemock sqlmock.Sqlmock
}

func (g *GormReadWrite) SetRead(db *gorm.DB) {
	g.read = db
}

func (g *GormReadWrite) SetWrite(db *gorm.DB) {
	g.write = db
}

func (g *GormReadWrite) SetReadMock(db sqlmock.Sqlmock) {
	g.readmock = db
}

func (g *GormReadWrite) SetWriteMock(db sqlmock.Sqlmock) {
	g.writemock = db
}

func (g *GormReadWrite) Read() *gorm.DB {
	return g.read
}

func (g *GormReadWrite) Write() *gorm.DB {
	return g.write
}

func (g *GormReadWrite) ReadMock() sqlmock.Sqlmock {
	return g.readmock
}

func (g *GormReadWrite) WriteMock() sqlmock.Sqlmock {
	return g.writemock
}

func (g *GormReadWrite) Close() error {
	e := g.read.Close()
	e2 := g.write.Close()
	if e != nil {
		return e
	}
	if e2 != nil {
		return e2
	}
	return nil
}

type Config interface {
	Get(path string) interface{}
	GetInt(path string) int
	GetString(path string) string
}

type Logger interface {
	ErrorF(category string, message string, args ...interface{})
	Print(v ...interface{})
}

type ErrorHandler interface {
	Error(e error)
}

type PathInterfaceGetter interface {
	Get(path string) interface{}
}
