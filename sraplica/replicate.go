package sraplica

import (
	"database/sql"
	"log"
	"strings"

	"github.com/go-sql-driver/mysql"
)

var DestDb *sql.DB
var DestConfig *mysql.Config

func InitDestDb() *sql.DB {
	DestConfig = GetDestConfig()

	destDb, err := sql.Open("mysql", DestConfig.FormatDSN())
	ErrorPanic(err)
	ErrorPanic(destDb.Ping())
	return destDb
}

func Update(cLog *ChangeLog) {
	if len(cLog.Keys) > 0 {
		var sqlBuilder strings.Builder
		var cols, keys []string
		var updateParams []interface{}
		sqlBuilder.WriteString(strings.Join([]string{"UPDATE", cLog.TableName, "SET "}, " "))
		for column := range cLog.Columns {
			val := cLog.Columns[column]
			custom := false
			if t, ok := Tables[cLog.TableName]; ok {
				v, cExists := t.Cols[column]
				if ok && cExists {
					val = v.Value
					custom = v.Custom
				}
			}

			if custom {
				cols = append(cols, strings.Join([]string{"`", column, "`=", val.(string)}, ""))
			} else {
				cols = append(cols, strings.Join([]string{column, "=?"}, ""))
				updateParams = append(updateParams, val)
			}
		}
		sqlBuilder.WriteString(strings.Join(cols, ", "))
		sqlBuilder.WriteString(" WHERE ")

		for wKey := range cLog.Keys {
			keys = append(keys, strings.Join([]string{wKey, "=?"}, ""))
			updateParams = append(updateParams, cLog.Keys[wKey])
		}
		sqlBuilder.WriteString(strings.Join(keys, " AND "))
		sql := sqlBuilder.String()
		if DestDb == nil {
			DestDb = InitDestDb()
		}
		stmt, err := DestDb.Prepare(sql)
		ErrorLog(err)

		//preparing params pending
		_, err = stmt.Exec(updateParams...)
		ErrorLog(err)
		// log.Println(sql)
	}
}

func Insert(cLog *ChangeLog) {
	var sqlBuilder strings.Builder
	var keys, values []string
	var insertParams []interface{}
	sqlBuilder.WriteString(strings.Join([]string{"INSERT INTO", cLog.TableName, "("}, " "))
	for column := range cLog.Columns {
		value := cLog.Columns[column]
		custom := false
		if t, ok := Tables[cLog.TableName]; ok {
			v, cExists := t.Cols[column]
			if ok && cExists {
				value = v.Value
				custom = v.Custom
			}
		}
		keys = append(keys, "`"+column+"`")
		if custom {
			values = append(values, value.(string))
		} else {
			values = append(values, "?")
			insertParams = append(insertParams, value)
		}
	}
	sqlBuilder.WriteString(strings.Join(keys, ", "))
	sqlBuilder.WriteString(") VALUES (")
	sqlBuilder.WriteString(strings.Join(values, ", "))
	sqlBuilder.WriteString(");")
	if DestDb == nil {
		DestDb = InitDestDb()
	}
	sql := sqlBuilder.String()
	// log.Println(sql)
	stmt, err := DestDb.Prepare(sql)
	ErrorLog(err)
	_, err = stmt.Exec(insertParams...)
	ErrorLog(err)

}

func Delete(cLog *ChangeLog) {
	var delParams []interface{}
	sql := strings.Join([]string{"DELETE FROM `", cLog.TableName, "` WHERE "}, "")
	for key := range cLog.Keys {
		sql = strings.Join([]string{sql, key, "=?"}, " ")
		delParams = append(delParams, cLog.Keys[key])
	}
	sql = strings.Join([]string{sql, ";"}, "")
	if DestDb == nil {
		DestDb = InitDestDb()
	}
	log.Println(sql)
	stmt, err := DestDb.Prepare(sql)
	ErrorLog(err)
	_, err = stmt.Exec(delParams...)
	ErrorLog(err)
}

func SchemaAltered(b []byte) {
	stmt, err := DestDb.Prepare(string(b))
	ErrorLog(err)
	_, err = stmt.Exec()
	ErrorLog(err)
}
