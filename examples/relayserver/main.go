package main

import (
	"flag"
	"fmt"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/psrpc"
	"github.com/nats-io/nats.go"
	"log"
	"os"
	"time"
)

const (
	defaultNatsHost = "localhost"     // 默认服务器地址
	defaultNatsPort = 4222            // 默认端口号
	defaultTimeout  = 5 * time.Second // 默认连接超时时间
)

func main() {
	// 定义命令行参数
	host := flag.String("nats-host", defaultNatsHost, "NATS server address (default to localhost)")
	port := flag.Int("port", defaultNatsPort, "NATS server port (default: 4222)")

	// 解析命令行参数
	flag.Parse()

	// 验证必需参数
	if *host == "" || *port == 0 {
		fmt.Println("Error: host and port must be set")
		flag.Usage()
		os.Exit(1)
	}

	// 检查端口范围
	if *port < 1 || *port > 65535 {
		fmt.Println("Error: port must be between 1 and 65535")
		os.Exit(1)
	}

	// 构建完整的服务器地址
	natsAddr := fmt.Sprintf("%s:%d", *host, *port)

	nc, err := nats.Connect(natsAddr)
	if err != nil {
		log.Fatalf("Connect nats-server failed! %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	bus := psrpc.NewNatsMessageBus(nc)

	conf, err := config.NewConfig("", true, nil, nil)
	conf.Limit.NumTracks = 10
	node, err := routing.NewLocalNode(conf)

	roomName := livekit.RoomName("room.*")
	signalRelayConfig := config.SignalRelayConfig{
		RetryTimeout:     7500 * time.Millisecond,
		MinRetryInterval: 500 * time.Millisecond,
		MaxRetryInterval: 4 * time.Second,
		StreamBufferSize: 1000,
		ConnectAttempts:  3,
	}
	server, err := service.NewDefaultCloudRelaySignalServer(node, roomName, bus, signalRelayConfig, nil)
	if err != nil {
		log.Fatalf("Failed to create server: %v\n", err)
		os.Exit(1)
	}

	err = server.Start()
	if err != nil {
		log.Fatalf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
	defer server.Stop()

	select {}
}
