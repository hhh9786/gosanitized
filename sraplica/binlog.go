package sraplica

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	c "github.com/siddontang/go-mysql/canal"
	m "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	// "github.com/siddontang/go-mysql/mysql"
	// "github.com/siddontang/go/log"
)

//MyEventHandler struct
type MyEventHandler struct {
	c.DummyEventHandler
}

//MyEvent struct
type MyEvent struct {
	c.RowsEvent
}

//IndexedArray -
type IndexedArray []interface{}

//AssociateArray -
type AssociateArray map[string]interface{}

//ChangeLog -
type ChangeLog struct {
	BinlogTimestamp uint32
	Keys            AssociateArray
	Timestamp       int64
	TableName       string
	ChangeType      string
	Columns         AssociateArray
}

var dataMap map[string]Table
var resourceMutex sync.Mutex
var Tables SensitiveTables
var SourceConfig *mysql.Config

func (r *MyEvent) getModifiedCols() AssociateArray {
	ret := make(AssociateArray)
	for index, val := range r.Table.Columns {
		//for text data types converting binary to string
		switch x := r.Rows[0][index].(type) {
		case []uint8:
			r.Rows[1][index] = string(x)
			r.Rows[0][index] = string(x)
		}

		if r.Rows[1][index] != r.Rows[0][index] {
			ret[val.Name] = r.Rows[1][index]
		}
	}
	return ret
}

func (r *MyEvent) getInsertedOrDeletedCols() AssociateArray {
	ret := make(AssociateArray)
	for index, val := range r.Rows[0] {
		ret[r.Table.Columns[index].Name] = val
	}
	return ret
}

func (r *MyEvent) getKeys() AssociateArray {
	ret := make(AssociateArray)
	for i := range r.Table.PKColumns {
		pkVal, _ := r.Table.GetPKValues(r.Rows[0])
		ret[r.Table.GetPKColumn(i).Name] = pkVal[0]
	}

	return ret
}

func (r *MyEvent) getKeyCondition() (string, []interface{}) {
	keyString := ""
	pkVal, _ := r.Table.GetPKValues(r.Rows[0])
	for i := range r.Table.PKColumns {
		keyString += r.Table.Name + "." + r.Table.GetPKColumn(i).Name + "=?"
	}
	return keyString, pkVal
}

// func (h *MyEventHandler) OnTableChanged(schema, table string) error {
// 	return nil
// }

//OnDDL will handle schema changes
func (h *MyEventHandler) OnDDL(nextPos m.Position, queryEvent *replication.QueryEvent) error {
	sql := bytes.ReplaceAll(queryEvent.Query, queryEvent.Schema, []byte(DestConfig.DBName))
	log.Println(string(sql))
	SchemaAltered(sql)
	return nil
}
func (h *MyEventHandler) OnRow(e *c.RowsEvent) error {
	// if e.Table.Schema c.DumpConfig.Databases

	if e.Table.Schema == SourceConfig.DBName {
		var me MyEvent = MyEvent{*e}
		// defer defferRowEvent()
		var cLog ChangeLog

		cLog = ChangeLog{
			BinlogTimestamp: me.Header.Timestamp,
			Timestamp:       time.Now().Unix(),
			Keys:            me.getKeys(),
			TableName:       me.Table.Name,
			ChangeType:      me.Action,
		}

		switch e.Action {
		case c.UpdateAction:
			cLog.Columns = me.getModifiedCols()
			Update(&cLog)

		case c.InsertAction:
			cLog.Columns = me.getInsertedOrDeletedCols()
			Insert(&cLog)
		case c.DeleteAction:
			Delete(&cLog)
		}

		log.Println(me.RowsEvent.Header.EventType, " for ", me.RowsEvent.Table)
	}
	return nil
}

func InitSync() {
	DestDb = InitDestDb()
	SourceConfig = GetSourceConfig()
	cfg := c.NewDefaultConfig()
	cfg.Addr = SourceConfig.Addr
	cfg.User = SourceConfig.User
	cfg.Password = SourceConfig.Passwd

	cnl, err := c.NewCanal(cfg)
	if err != nil {
		log.Println(err.Error())
	}
	// Register a handler to handle RowsEvent
	cnl.SetEventHandler(&MyEventHandler{})

	// Start canal
	pos, _ := cnl.GetMasterPos()
	//mysql-bin.000066, 9127
	// pos := mysql.Position{"mysql-bin.000052", 120}
	cnl.RunFrom(pos)
	// c.Run()
}
