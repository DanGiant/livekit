package service

import (
	"context"
	"fmt"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rpc"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/metadata"
	"github.com/pkg/errors"
	"log"
)

type CloudRelayService struct {
	region         string
	nodeID         livekit.NodeID
	sessionHandler RelaySessionHandler
	config         config.SignalRelayConfig
}

func (s *CloudRelayService) RoomOnline(ctx context.Context, req *rpc.RoomOnlineRequest) (*rpc.RoomOnlineResponse, error) {
	fmt.Printf("Receive RoomOnline request for room: %s from node:%s\n", req.RoomName, req.NodeId)

	err := s.sessionHandler.HandleRoomOnline(ctx, livekit.RoomName(req.RoomName), livekit.NodeID(req.NodeId))
	if err != nil {
		return &rpc.RoomOnlineResponse{
			RoomName: req.RoomName,
			NodeId:   string(s.nodeID),
			Exist:    false,
		}, nil
	} else {
		return &rpc.RoomOnlineResponse{
			RoomName: req.RoomName,
			NodeId:   string(s.nodeID),
			Exist:    true,
		}, nil
	}
}

func (s *CloudRelayService) RoomOffline(ctx context.Context, req *rpc.RoomOfflineRequest) (*rpc.RoomOfflineResponse, error) {
	fmt.Printf("Receive RoomOffline request for room: %s from node:%s\n", req.RoomName, req.NodeId)
	err := s.sessionHandler.HandleRoomOffline(ctx, livekit.RoomName(req.RoomName), livekit.NodeID(req.NodeId))
	if err != nil {
		return &rpc.RoomOfflineResponse{
			RoomName: req.RoomName,
			NodeId:   string(s.nodeID),
			Exist:    false,
		}, nil
	} else {
		return &rpc.RoomOfflineResponse{
			RoomName: req.RoomName,
			NodeId:   string(s.nodeID),
			Exist:    true,
		}, nil
	}
}

func (s *CloudRelayService) RoomSignalRelay(stream psrpc.ServerStream[*rpc.RoomSignalRelayResponse, *rpc.RoomSignalRelayRequest]) error {
	req, ok := <-stream.Channel()
	if !ok {
		log.Fatalf("failed to read channel")
		return nil
	}

	srs := req.StartRelaySession
	if srs == nil {
		return errors.New("expected start relay session message")
	}

	pi, err := routing.ParticipantInitFromStartRelaySession(srs, s.region)
	if err != nil {
		return errors.Wrap(err, "failed to read participant from session")
	}

	l := s.sessionHandler.Logger(stream.Context()).WithValues(
		"room", srs.RoomName,
		"participant", srs.Identity,
		"connID", srs.ConnectionId,
	)

	stream.Hijack()
	sink := routing.NewSignalMessageSink(routing.SignalSinkParams[*rpc.RoomSignalRelayResponse, *rpc.RoomSignalRelayRequest]{
		Logger:       l,
		Stream:       stream,
		Config:       s.config,
		Writer:       roomSignalRelayResponseMessageWriter{},
		ConnectionID: livekit.ConnectionID(srs.ConnectionId),
	})
	reqChan := routing.NewDefaultMessageChannel(livekit.ConnectionID(srs.ConnectionId))

	go func() {
		err := routing.CopySignalStreamToMessageChannel[*rpc.RoomSignalRelayResponse, *rpc.RoomSignalRelayRequest](
			stream,
			reqChan,
			roomSignalRelayRequestMessageReader{},
			s.config,
		)
		l.Debugw("relay signal stream closed", "error", err)

		reqChan.Close()
	}()

	// copy the context to prevent a race between the session handler closing
	// and the delivery of any parting messages from the client. take care to
	// copy the incoming rpc headers to avoid dropping any session vars.
	ctx := metadata.NewContextWithIncomingHeader(context.Background(), metadata.IncomingHeader(stream.Context()))
	err = s.sessionHandler.HandleRelaySession(ctx, *pi, livekit.ConnectionID(srs.ConnectionId), reqChan, sink)
	if err != nil {
		sink.Close()
		l.Errorw("could not handle relay session", err)
	}
	return err
}
