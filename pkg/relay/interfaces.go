package relay

import (
	"context"
	"encoding/json"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rpc"
	"github.com/livekit/livekit-server/pkg/utils"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"go.uber.org/zap/zapcore"
)

type ParticipantRelayInit struct {
	RoomName             livekit.RoomName
	FromNode             livekit.NodeID
	ToNode               livekit.NodeID
	ID                   livekit.ParticipantID
	Identity             livekit.ParticipantIdentity
	Name                 livekit.ParticipantName
	Reconnect            bool
	ReconnectReason      livekit.ReconnectReason
	AutoSubscribe        bool
	Client               *livekit.ClientInfo
	Grants               *auth.ClaimGrants
	Region               string
	AdaptiveStream       bool
	SubscriberAllowPause *bool
	DisableICELite       bool
}

func (pri *ParticipantRelayInit) MarshalLogObject(e zapcore.ObjectEncoder) error {
	if pri == nil {
		return nil
	}

	logBoolPtr := func(prop string, val *bool) {
		if val == nil {
			e.AddString(prop, "not-set")
		} else {
			e.AddBool(prop, *val)
		}
	}

	e.AddString("RoomName", string(pri.RoomName))
	e.AddString("FromNode", string(pri.FromNode))
	e.AddString("ToNode", string(pri.ToNode))

	e.AddString("ID", string(pri.ID))
	e.AddString("Identity", string(pri.Identity))
	logBoolPtr("Reconnect", &pri.Reconnect)
	e.AddString("ReconnectReason", pri.ReconnectReason.String())
	logBoolPtr("AutoSubscribe", &pri.AutoSubscribe)
	e.AddObject("Client", logger.Proto(utils.ClientInfoWithoutAddress(pri.Client)))
	e.AddObject("Grants", pri.Grants)
	e.AddString("Region", pri.Region)
	logBoolPtr("AdaptiveStream", &pri.AdaptiveStream)
	logBoolPtr("SubscriberAllowPause", pri.SubscriberAllowPause)
	logBoolPtr("DisableICELite", &pri.DisableICELite)
	return nil
}

func (pri *ParticipantRelayInit) ToStartRelaySession(roomName livekit.RoomName, connectionID livekit.ConnectionID) (*rpc.StartRelaySession, error) {
	claims, err := json.Marshal(pri.Grants)
	if err != nil {
		return nil, err
	}

	srs := &rpc.StartRelaySession{
		RoomName:      string(roomName),
		ParticipantId: string(pri.ID),
		Identity:      string(pri.Identity),
		Name:          string(pri.Name),
		// connection id is to allow the RTC node to identify where to route the message back to
		ConnectionId:    string(connectionID),
		FromNode:        string(pri.FromNode),
		Reconnect:       pri.Reconnect,
		ReconnectReason: pri.ReconnectReason,
		AutoSubscribe:   pri.AutoSubscribe,
		Client:          pri.Client,
		GrantsJson:      string(claims),
		AdaptiveStream:  pri.AdaptiveStream,
		DisableIceLite:  pri.DisableICELite,
		Region:          pri.Region,
	}
	if pri.SubscriberAllowPause != nil {
		subscriberAllowPause := *pri.SubscriberAllowPause
		srs.SubscriberAllowPause = &subscriberAllowPause
	}

	return srs, nil
}

type StartParticipantRelaySignalResults struct {
	ConnectionID        livekit.ConnectionID
	RequestSink         routing.MessageSink
	ResponseSource      routing.MessageSource
	DstNodeID           livekit.NodeID
	NodeSelectionReason string
}

type RoomRelayRouter interface {
	RoomOnline(ctx context.Context, roomName livekit.RoomName) (res *[]livekit.NodeID, err error)
	RoomOffline(ctx context.Context, roomName livekit.RoomName) (err error)
	// StartParticipantRelaySignal participant relay signal connection is ready to start
	StartParticipantRelaySignal(ctx context.Context, roomName livekit.RoomName, toNode livekit.NodeID, pri ParticipantRelayInit) (res StartParticipantRelaySignalResults, err error)
}
