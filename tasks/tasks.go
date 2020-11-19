package tasks

import (
	"squirrel/buffer"
	"squirrel/cache"
	"squirrel/config"
	"squirrel/db"
	"squirrel/log"
	"squirrel/rpc"
)

// Run starts several goroutines for block storage, tx/nep5 tx storage, etc.
func Run(height int) {
	log.Printf("Init addr asset cache.")

	// Init cache to speed up db queries
	addrAssetInfo := db.GetAddrAssetInfo()
	cache.LoadAddrAssetInfo(addrAssetInfo)
	// dbHeight := db.GetLastHeight()
	initTask(height)
	for i := 0; i < config.GetGoroutines(); i++ {
		go fetchBlock()
	}
	blockChannel = make(chan *rpc.RawBlock, bufferSize)
	go arrangeBlock(height, blockChannel)
	go storeBlock(blockChannel)

	go startNep5Task()
	go startTxTask()
	go startUpdateCounterTask()

	// go startNftTask()
	// go startAssetTxTask()
	// go startGasBalanceTask()
	// go startSCTask()
}

func initTask(dbHeight int) {
	blockBuffer = buffer.NewBuffer(dbHeight)
	bestHeight := rpc.RefreshServers()

	log.Printf("Current params for block persistance:\n")
	log.Printf("\tdb block height = %d\n", dbHeight)
	log.Printf("\trpc best height = %d\n", bestHeight)
}
