package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var flagCfgPath string

func init() {
}

type ApplicationCfg struct {
}

type Application struct {
	GRpc *grpc.Server
	lg   *zap.Logger
}

func (app *Application) Init() {
	lg, _ := zap.NewDevelopment()
	app.lg = lg
	for _, args := range os.Args {
		kvs := strings.Split(args, "=")
		if len(kvs) == 2 {
			if kvs[0] == "config" {
				flagCfgPath = kvs[1]
				break
			}
		}
	}

	//app.lg.Sugar().Infow("config path", "path", flagCfgPath)
}

func (app *Application) InitGRpcServer() {
	app.GRpc = grpc.NewServer()
}

func (app *Application) InitConfig(cfgPath string, cfg interface{}) {
	if cfgPath != "" {
		flagCfgPath = cfgPath
	}

	jf, err := os.Open(flagCfgPath)
	if err != nil {
		panic(fmt.Errorf("%v %s", err, flagCfgPath))
	}
	defer jf.Close()

	byteValue, _ := ioutil.ReadAll(jf)
	if err := json.Unmarshal([]byte(byteValue), cfg); err != nil {
		panic(err)
	}
	app.lg.Sugar().Infow("load config", "path", flagCfgPath, "cfg", cfg)
}

func (app *Application) RunGRpcServer(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}
	app.GRpc.Serve(lis)
}

func (app *Application) Dial(addr string) (*grpc.ClientConn, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
		grpc_zap.UnaryClientInterceptor(app.lg))))
	return cc, err
}
