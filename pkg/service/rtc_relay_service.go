package service

import (
	"errors"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rpc"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/psrpc"
	"github.com/nats-io/nats.go"
	"time"
)

var ErrRtcRelayNotEnabled = errors.New("RTC relay is not enabled")
var ErrNatsNotConfigured = errors.New("Nats is not configured")

type RTCRelayService struct {
	natsConn *nats.Conn
	bus      psrpc.MessageBus
	server   *CloudRelaySignalServer
	client   rpc.TypedCloudRelayServiceClient
}

func getNatsConnection(conf *config.NatsConfig) (*nats.Conn, error) {
	if conf == nil {
		return nil, ErrNatsNotConfigured
	}

	if !conf.IsConfigured() {
		return nil, ErrNatsNotConfigured
	}

	option := func(options *nats.Options) error {
		options.AllowReconnect = true
		options.MaxReconnect = -1
		options.ReconnectWait = 15 * time.Second
		return nil
	}

	conn, err := nats.Connect(conf.Address, option)
	return conn, err
}

func getNatsMessageBus(nc *nats.Conn) psrpc.MessageBus {
	return psrpc.NewNatsMessageBus(nc)
}

func NewRTCRelayService(conf config.RtcRelayConfig, node routing.LocalNode, roomManager *RoomManager) (*RTCRelayService, error) {
	if !conf.Enabled {
		return nil, nil
	}

	natsConn, err := getNatsConnection(&conf.Nats)
	if err != nil {
		return nil, err
	}

	bus := getNatsMessageBus(natsConn)

	roomName := livekit.RoomName("room.*")
	signalRelayConfig := config.SignalRelayConfig{
		RetryTimeout:     7500 * time.Millisecond,
		MinRetryInterval: 500 * time.Millisecond,
		MaxRetryInterval: 4 * time.Second,
		StreamBufferSize: 1000,
		ConnectAttempts:  3,
	}
	server, err := NewDefaultCloudRelaySignalServer(node, roomName, bus, signalRelayConfig, roomManager)
	if err != nil {
		return nil, err
	}

	client, err := rpc.NewTypedCloudRelayServiceClient(node.NodeID(), roomName, bus)
	if err != nil {
		return nil, err
	}

	return &RTCRelayService{
		natsConn: natsConn,
		bus:      bus,
		server:   server,
		client:   client,
	}, nil
}
