package main

import (
	"myDB/server/network"
	"myDB/server/utils"
)

func main() {
	// Server
	s := network.NewServer("tcp4")
	// Database
	db := network.NewDbRouter(utils.GlobalObj.Path, utils.GlobalObj.BufferPoolMemory, utils.GlobalObj.Iso)
	s.AddRouter(network.DbRouterMsgId, db)

	utils.GlobalObj.TcpServer = s
	s.Serve()
}
