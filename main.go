package main

import (
	s "github.com/hhh9786/gosanitized/sraplica"
	"log"
	"os"
	"os/signal"
)

var tables s.SensitiveTables

func CloseConnectionPoolsOnOSIntruption() {
	c := make(chan os.Signal)
	// signal.Notify(c, os.Kill)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		close(c)
		signal.Stop(c)
		s.DestDb.Close()
	}()
}

func main() {
	defer func() {
		// s.DestDb.Close()
	}()

	configDir := "192.168.56.102:3306"
	log.Println("Sanitized raplication started")
	if val, envSet := os.LookupEnv("configDir"); envSet {
		configDir = val
	}
	s.Tables = s.GetTablesToFilter(configDir)

	// s.InitRaplication()
	s.InitSync()
}
