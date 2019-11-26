package sraplica

import (
	"context"
	"os"

	"github.com/go-sql-driver/mysql"
	c "github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
)

func InitRaplication() {
	DestDb = InitDestDb()
	var sourceConf *mysql.Config
	sourceConf = GetSourceConfig()
	config := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "192.168.56.102",
		Port:     3306,
		User:     sourceConf.User,
		Password: sourceConf.Passwd,
	}
	syncer := replication.NewBinlogSyncer(config)

	cfg := c.NewDefaultConfig()
	cfg.Addr = sourceConf.Addr
	cfg.User = sourceConf.User
	cfg.Password = sourceConf.Passwd

	cnl, err := c.NewCanal(cfg)
	ErrorLog(err)
	pos, _ := cnl.GetMasterPos()
	streamer, err := syncer.StartSync(pos)
	ErrorLog(err)
	for {
		ev, _ := streamer.GetEvent(context.Background())

		// Dump event
		ev.Dump(os.Stdout)
	}
}
