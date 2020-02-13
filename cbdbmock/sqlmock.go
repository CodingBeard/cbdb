package cbdbmysql

import (
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/codingbeard/cbconfig/require"
	"github.com/codingbeard/cbdb"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

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

func NewGormSqlMockReadWrite(config cbdb.Config, logger cbdb.Logger, errorHandler cbdb.ErrorHandler, cbNamingStyle bool) (*cbdb.GormReadWrite, error) {
	readWrite := &cbdb.GormReadWrite{}

	es := VerifyConfig(config)
	if len(es) != 0 {
		for _, e := range es {
			logger.ErrorF("DATABASE", e.Error())
		}

		e := errors.New("required configs not found")
		errorHandler.Error(e)
		return readWrite, e
	}

	if cbNamingStyle {
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

	db, readmock, e := sqlmock.New()

	read, e := gorm.Open("mysql", db)
	if e != nil {
		errorHandler.Error(e)
		return readWrite, e
	}
	if cbNamingStyle {
		read.SingularTable(true)
	}
	read.SetLogger(logger)
	read.DB().SetConnMaxLifetime(time.Hour)

	db, writemock, e := sqlmock.New()

	write, e := gorm.Open("mysql", db)
	if e != nil {
		errorHandler.Error(e)
		return readWrite, e
	}
	if cbNamingStyle {
		write.SingularTable(true)
	}
	write.SetLogger(logger)
	write.DB().SetConnMaxLifetime(time.Hour)

	readWrite.SetRead(read)
	readWrite.SetReadMock(readmock)
	readWrite.SetWrite(write)
	readWrite.SetWriteMock(writemock)

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
