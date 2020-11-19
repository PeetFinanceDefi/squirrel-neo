package main

import (
	"flag"
	_ "net/http/pprof"
	"squirrel/config"
	"squirrel/db"
	"squirrel/log"
	"squirrel/rpc"
	"squirrel/tasks"
	"time"
	// "squirrel/tasks"
)

var enableMail bool

func init() {
	flag.BoolVar(&enableMail, "mail", false, "If mail alert is enabled")
}

func main() {
	flag.Parse()

	log.Init()
	config.Load(false)
	db.Init()

	go rpc.TraceBestHeight()
	log.Println("Waiting for chain last height..")
	lastHeight := 0
	for {
		lastHeight = rpc.BestHeight.Get()
		if lastHeight != 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	// cause we wanna look up on the last 50 blocks in case we missed something
	// lastHeight -= 50

	log.Printf("Chain lastHeight loaded: %d\n", lastHeight)
	tasks.Run(lastHeight)

	select {}
}
