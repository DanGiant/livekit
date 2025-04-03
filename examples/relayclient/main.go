package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/relay"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rpc"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/protocol/utils/guid"
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
		log.Fatalf("Connect nats-server failed! %v", err)
		os.Exit(1)
	}
	defer nc.Close()

	bus := psrpc.NewNatsMessageBus(nc)

	conf, err := config.NewConfig("", true, nil, nil)
	conf.Limit.NumTracks = 10
	node, err := routing.NewLocalNode(conf)
	localNode := node.NodeID()

	roomName := livekit.RoomName("room1")

	client, err := rpc.NewTypedCloudRelayServiceClient(localNode, livekit.RoomName("room.*"), bus)
	if err != nil {
		log.Fatalf("create client failed! %v", err)
		os.Exit(1)
	}
	defer client.Close()

	signalRelayConfig := config.SignalRelayConfig{
		RetryTimeout:     7500 * time.Millisecond,
		MinRetryInterval: 500 * time.Millisecond,
		MaxRetryInterval: 4 * time.Second,
		StreamBufferSize: 1000,
		ConnectAttempts:  3,
	}
	relayClient, err := relay.NewCloudRelaySignalClientFromTypedClient(localNode, roomName, bus, signalRelayConfig, client)
	if err != nil {
		log.Fatalf("create relay client failed! %v", err)
		os.Exit(1)
	}

	log.Printf("Broadcasting RoomOnline...\n")

	nodes, err := relayClient.RoomOnline(context.Background(), livekit.RoomName(roomName))
	if err != nil {
		log.Fatalf("Send RoomOnline failed! %v", err)
		os.Exit(1)
	}

	if len(*nodes) == 0 {
		log.Fatalf("No online nodes found!")
		os.Exit(1)
	}

	log.Printf("RoomOnline echo %d room(s)\n", len(*nodes))

	for _, toNode := range *nodes {
		go func() {
			sid := livekit.ParticipantID(guid.New(utils.ParticipantPrefix))
			log.Printf("Trying to create relay signal to node: %s for participant: %s\n", string(toNode), string(sid))

			pri := relay.ParticipantRelayInit{
				RoomName:       roomName,
				FromNode:       localNode,
				ToNode:         toNode,
				ID:             sid,
				Identity:       "user01",
				Name:           "user01",
				Reconnect:      false,
				AutoSubscribe:  true,
				AdaptiveStream: false,
				DisableICELite: true,
			}
			_, reqSink, resSource, err := relayClient.StartParticipantRelaySignal(context.Background(), roomName, toNode, pri)
			if err != nil {
				log.Fatalf("start relay signal client failed! %v", err)
				os.Exit(1)
			}
			done := make(chan struct{})

			defer func() {
				reqSink.Close()
				resSource.Close()

				close(done)
			}()

			go func() {
				for {
					select {
					case <-done:
						return
					case msg := <-resSource.ReadChan():
						if msg == nil {
							log.Printf("Receive message failed!")
							continue
						}

						res, ok := msg.(*rpc.RelayedSignalResponse)
						if !ok {
							log.Printf("Parse message failed!")
							continue
						}

						switch res.Message.(type) {
						case *rpc.RelayedSignalResponse_Offer:
							log.Printf("receive offer response: %v\n", res.Message)
						case *rpc.RelayedSignalResponse_Answer:
							log.Printf("receive answer response: %v\n", res.Message)
						case *rpc.RelayedSignalResponse_PongResp:
							log.Printf("receive pong response: %v\n", res.Message)
						}
					}
				}
			}()

			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					reqMessageIn := &rpc.RelayedSignalRequest{
						Message: &rpc.RelayedSignalRequest_PingReq{PingReq: &livekit.Ping{
							Timestamp: time.Now().UnixMilli(),
						}},
					}
					log.Printf("Send a ping")
					err = reqSink.WriteMessage(reqMessageIn)
					if err != nil {
						log.Fatalf("WriteMessage failed! %v", err)
					}
				}
			}
		}()
	}

	select {}
}
