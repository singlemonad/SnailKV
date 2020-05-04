package common

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
)

type ApplicationCfg struct {
	GRpcAddress string
	GRpcPort    int
}

type Application struct {
	GRpc *grpc.Server
}

func (app *Application) Init() {

}

func (app *Application) InitGRpcServer() {

}

func (app *Application) RunGRpcServer(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	app.GRpc.Serve(lis)
}
