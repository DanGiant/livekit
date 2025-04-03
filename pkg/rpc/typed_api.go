package rpc

import (
	"fmt"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/rpc"
	"github.com/livekit/psrpc"
)

type TypedCloudRelayServiceClient = CloudRelayServiceClient[RoomTopic, livekit.NodeID]
type TypedCloudRelayServiceServer = CloudRelayServiceServer[RoomTopic, livekit.NodeID]

func NewTypedCloudRelayServiceClient(nodeID livekit.NodeID, roomName livekit.RoomName,
	bus psrpc.MessageBus, opts ...psrpc.ClientOption) (TypedCloudRelayServiceClient, error) {
	return NewCloudRelayServiceClient[RoomTopic, livekit.NodeID](bus, psrpc.WithClientOptions(opts...), psrpc.WithClientID(string(nodeID)))
}

func NewTypedCloudRelayServiceServer(nodeID livekit.NodeID, roomName livekit.RoomName,
	svc CloudRelayServiceServerImpl, bus psrpc.MessageBus, opts ...psrpc.ServerOption) (TypedCloudRelayServiceServer, error) {
	server, err := NewCloudRelayServiceServer[RoomTopic, livekit.NodeID](svc, bus, psrpc.WithServerOptions(opts...), psrpc.WithServerID(string(nodeID)))
	if err == nil {
		roomTopic := rpc.FormatRoomTopic(roomName)
		err = server.RegisterAllRoomTopics(RoomTopic(roomTopic))
	}
	return server, err
}

type ParticipantTopic string
type RoomTopic string

func FormatParticipantTopic(roomName livekit.RoomName, identity livekit.ParticipantIdentity) ParticipantTopic {
	return ParticipantTopic(fmt.Sprintf("%s_%s", roomName, identity))
}

func FormatRoomTopic(roomName livekit.RoomName) RoomTopic {
	return RoomTopic(roomName)
}
