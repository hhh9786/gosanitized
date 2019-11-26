package sraplica

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/go-sql-driver/mysql"
)

type Column struct {
	Value  string
	Custom bool
}

type Table struct {
	Name string
	Cols map[string]*Column
}

//ErrorPanic -
func ErrorPanic(err error) {
	if err != nil {
		log.Panicln(err.Error())
	}
}

func ErrorLog(err error) error {
	if err != nil {
		log.Println(err.Error())
	}
	return err
}

type SensitiveTables map[string]*Table

func GetTablesToFilter(confDir string) SensitiveTables {

	os.Chdir(confDir)
	tables := make(SensitiveTables)

	filepath.Walk(".", func(filename string, fileInfo os.FileInfo, err error) error {
		ErrorPanic(err)

		if filename != "." && !fileInfo.IsDir() {
			var table Table

			contentBytes, readError := ioutil.ReadFile(filename)
			if readError != nil {
				panic(readError.Error())
			}
			err := json.Unmarshal(contentBytes, &table)
			ErrorLog(err)
			tables[table.Name] = &table
		}
		return nil
	})

	return tables
}

func getEnvOrDie(key string) (string, bool) {
	if val, envSet := os.LookupEnv(key); envSet {
		return val, true
	} else {
		return "", false
	}
}

//GetSourceConfig - gets config variables from environment variables or die
func GetSourceConfig() *mysql.Config {
	var config mysql.Config
	if v, ok := getEnvOrDie("host"); ok {
		config.Addr = v
	} else {
		config.Addr = "192.168.56.102:3306"
	}
	if v, ok := getEnvOrDie("user"); ok {
		config.User = v
	} else {
		config.User = "user1"
	}
	if v, ok := getEnvOrDie("password"); ok {
		config.Passwd = v
	} else {
		config.Passwd = "password1"
	}
	if v, ok := getEnvOrDie("dbname"); ok {
		config.DBName = v
	} else {
		config.DBName = "db1_name"
	}
	config.AllowNativePasswords = true
	config.Net = "tcp"

	return &config
}

//GetSourceConfig - gets config variables from environment variables or die
func GetDestConfig() *mysql.Config {
	var config mysql.Config
	if v, ok := getEnvOrDie("dhost"); ok {
		config.Addr = v
	} else {
		config.Addr = "192.168.56.102:3306"
	}
	if v, ok := getEnvOrDie("duser"); ok {
		config.User = v
	} else {
		config.User = "user2"
	}
	if v, ok := getEnvOrDie("dpassword"); ok {
		config.Passwd = v
	} else {
		config.Passwd = "password2"
	}
	if v, ok := getEnvOrDie("ddbname"); ok {
		config.DBName = v
	} else {
		config.DBName = "db2_name"
	}
	config.AllowNativePasswords = true
	config.Net = "tcp"

	return &config
}
