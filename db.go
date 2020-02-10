package cbdb

import (
	"errors"
	"fmt"
	"github.com/codingbeard/cbconfig/require"
	"github.com/jinzhu/gorm"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

type GormReadWrite struct {
	read  *gorm.DB
	write *gorm.DB
}

func (g *GormReadWrite) Read() *gorm.DB {
	return g.read
}

func (g *GormReadWrite) Write() *gorm.DB {
	return g.write
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

func VerifyConfig(config require.PathInterfaceGetter) []error {
	required := require.New()

	required.RequireString("mysql.read.user")
	required.RequireString("mysql.read.password")
	required.RequireString("mysql.read.host")
	required.RequireInt("mysql.read.port")
	required.RequireString("mysql.write.user")
	required.RequireString("mysql.write.password")
	required.RequireString("mysql.write.host")
	required.RequireInt("mysql.write.port")

	return required.Verify(config)
}

func NewGormReadWrite(config Config, logger Logger, errorHandler ErrorHandler, cbStyleNaming bool) (*GormReadWrite, error) {
	readWrite := &GormReadWrite{}

	es := VerifyConfig(config)
	if len(es) != 0 {
		for _, e := range es {
			logger.ErrorF("DATABASE", e.Error())
		}

		e := errors.New("required configs not found")
		errorHandler.Error(e)
		return readWrite, e
	}

	if cbStyleNaming {
		gorm.DefaultTableNameHandler = func(db *gorm.DB, defaultTableName string) string {
			parts := strings.Split(defaultTableName, "_")
			if len(parts) < 2 {
				return defaultTableName
			}
			return fmt.Sprintf("%s.%s", parts[0], strings.Join(parts[1:], "_"))
		}
		gorm.AddNamingStrategy(&gorm.NamingStrategy{
			Column: func(s string) string {
				return stringFirstLetterLower(s)
			},
		})
	}

	var e error
	readWrite.read, e = gorm.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?charset=utf8&parseTime=True&loc=Local",
		config.GetString("mysql.read.user"),
		config.GetString("mysql.read.password"),
		config.GetString("mysql.read.host"),
		config.GetInt("mysql.read.port"),
	))
	if e != nil {
		errorHandler.Error(e)
		return readWrite, e
	}
	if cbStyleNaming {
		readWrite.read.SingularTable(true)
	}
	readWrite.read.SetLogger(logger)
	readWrite.read.DB().SetConnMaxLifetime(time.Hour)

	readWrite.write, e = gorm.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?charset=utf8&parseTime=True&loc=Local",
		config.GetString("mysql.write.user"),
		config.GetString("mysql.write.password"),
		config.GetString("mysql.write.host"),
		config.GetInt("mysql.write.port"),
	))
	if e != nil {
		errorHandler.Error(e)
		return readWrite, e
	}
	if cbStyleNaming {
		readWrite.write.SingularTable(true)
	}
	readWrite.write.SetLogger(logger)
	readWrite.write.DB().SetConnMaxLifetime(time.Hour)

	return readWrite, e
}

func stringFirstLetterLower(s string) string {
	if len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r != utf8.RuneError || size > 1 {
			lo := unicode.ToLower(r)
			if lo != r {
				s = string(lo) + s[size:]
			}
		}
	}
	return s
}
