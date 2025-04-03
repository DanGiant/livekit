package relay

import (
	"context"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	rpc "github.com/livekit/livekit-server/pkg/rpc"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	lrpc "github.com/livekit/protocol/rpc"
	"github.com/livekit/protocol/utils/guid"
	"github.com/livekit/psrpc"
	"github.com/livekit/psrpc/pkg/middleware"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

//counterfeiter:generate . SessionHandler
type RelaySessionHandler interface {
	Logger(ctx context.Context) logger.Logger

	HandleRelaySession(
		ctx context.Context,
		//pi routing.ParticipantInit,
		connectionID livekit.ConnectionID,
		requestSource routing.MessageSource,
		responseSink routing.MessageSink,
	) error
}

type defaultRelaySessionHandler struct {
	currentNode routing.LocalNode
	currentRoom livekit.RoomName
	roomManager *service.RoomManager
}

func (s *defaultRelaySessionHandler) Logger(ctx context.Context) logger.Logger {
	return logger.GetLogger()
}

func (s *defaultRelaySessionHandler) HandleRelaySession(
	ctx context.Context,
	//pi routing.ParticipantInit,
	connectionID livekit.ConnectionID,
	requestSource routing.MessageSource,
	responseSink routing.MessageSink,
) error {
	//	prometheus.IncrementParticipantRtcInit(1)

	//rtcNode, err := s.router.GetNodeForRoom(ctx, livekit.RoomName(pi.CreateRoom.Name))
	//if err != nil {
	//	return err
	//}
	//
	//if livekit.NodeID(rtcNode.Id) != s.currentNode.NodeID() {
	//	err = routing.ErrIncorrectRTCNode
	//	logger.Errorw("called participant on incorrect node", err,
	//		"rtcNode", rtcNode,
	//	)
	//	return err
	//}
	//
	//return s.roomManager.StartSession(ctx, pi, requestSource, responseSink, false)
	go func() {
		defer func() {
			requestSource.Close()
		}()

		respTicker := time.NewTicker(5 * time.Second)
		defer respTicker.Stop()

		for {
			select {
			case <-respTicker.C:
				log.Printf("timed out while waiting for signal response")
				return
			case msg := <-requestSource.ReadChan():
				if msg == nil {
					log.Printf("No message! Close relay session's request channel")
					return
				}

				req := msg.(*livekit.SignalRequest)
				switch req.GetMessage().(type) {
				case *livekit.SignalRequest_Offer:
					log.Printf("Received offer")
				case *livekit.SignalRequest_Answer:
					log.Printf("Received answer")
				case *livekit.SignalRequest_PingReq:
					log.Printf("Received ping req")
					respTicker.Stop()
					respTicker = time.NewTicker(5 * time.Second)
					go func() {
						resMessageOut := &livekit.SignalResponse{
							Message: &livekit.SignalResponse_PongResp{PongResp: &livekit.Pong{
								Timestamp: time.Now().UnixMilli(),
							}},
						}
						log.Printf("Writing pong response...")
						err := responseSink.WriteMessage(resMessageOut)
						if err != nil {
							log.Printf("Error writing response: %v", err)
						}
						log.Printf("Written pong response!")
					}()
				}
			}
		}
	}()

	return nil
}

//counterfeiter:generate . SignalClient
type CloudRelaySignalClient interface {
	ActiveCount() int
	RoomOnline(ctx context.Context, roomName livekit.RoomName) (nodes *[]livekit.NodeID, err error)
	RoomOffline(ctx context.Context, roomName livekit.RoomName) (nodes *[]livekit.NodeID, err error)
	StartParticipantRelaySignal(ctx context.Context, roomName livekit.RoomName, toNode livekit.NodeID, pri ParticipantRelayInit) (connectionID livekit.ConnectionID, reqSink routing.MessageSink, resSource routing.MessageSource, err error)
}

type cloudRelaySignalClient struct {
	localNode livekit.NodeID
	roomName  livekit.RoomName
	config    config.SignalRelayConfig
	client    rpc.TypedCloudRelayServiceClient
	active    atomic.Int32
}

func NewCloudRelaySignalClientFromTypedClient(nodeID livekit.NodeID, roomName livekit.RoomName, bus psrpc.MessageBus,
	config config.SignalRelayConfig, c rpc.TypedCloudRelayServiceClient) (CloudRelaySignalClient, error) {
	return &cloudRelaySignalClient{
		localNode: nodeID,
		roomName:  roomName,
		config:    config,
		client:    c,
	}, nil
}

func NewCloudRelaySignalClient(nodeID livekit.NodeID, roomName livekit.RoomName, bus psrpc.MessageBus,
	config config.SignalRelayConfig) (CloudRelaySignalClient, error) {
	c, err := rpc.NewTypedCloudRelayServiceClient(
		nodeID, roomName, bus,
		middleware.WithClientMetrics(lrpc.PSRPCMetricsObserver{}),
		psrpc.WithClientChannelSize(config.StreamBufferSize),
	)
	if err != nil {
		return nil, err
	}

	return &cloudRelaySignalClient{
		localNode: nodeID,
		roomName:  roomName,
		config:    config,
		client:    c,
	}, nil
}

func (r *cloudRelaySignalClient) ActiveCount() int {
	return int(r.active.Load())
}

func (r *cloudRelaySignalClient) RoomOnline(ctx context.Context, roomName livekit.RoomName) (nodes *[]livekit.NodeID, err error) {
	req := &rpc.RoomOnlineRequest{
		RoomName: string(roomName),
		Node: &livekit.Node{
			Id: string(r.localNode),
		},
	}
	res, err := r.client.RoomOnline(context.Background(), rpc.RoomTopic("room.*"), req)
	if err != nil {
		logger.Errorw("RoomOnline failed! ", err,
			"roomName", roomName)
		log.Printf("RoomOnline failed! error: %v", err)
		return nil, err
	}

	n := make([]livekit.NodeID, 0)
	for {
		resp := <-res
		if resp == nil {
			logger.Debugw("RoomOnline response channel closed", "roomName", roomName)
			break
		}

		if resp.Result.RoomName == req.RoomName && resp.Result.NodeId != string(r.localNode) {
			n = append(n, livekit.NodeID(resp.Result.NodeId))
		}
	}

	//if len(n) == 0 {
	//	return nil, service.ErrRoomNotFound
	//} else {
	//	return &n, nil
	//}
	return &n, nil
}

func (r *cloudRelaySignalClient) RoomOffline(ctx context.Context, roomName livekit.RoomName) (nodes *[]livekit.NodeID, err error) {
	req := &rpc.RoomOfflineRequest{
		RoomName: string(roomName),
		NodeId:   string(r.localNode),
	}
	res, err := r.client.RoomOffline(context.Background(), rpc.RoomTopic("room.*"), req)
	if err != nil {
		logger.Errorw("RoomOffline failed! ", err,
			"roomName", roomName)
		log.Printf("RoomOffline failed! error: %v", err)
		return nil, err
	}

	n := make([]livekit.NodeID, 0)
	for {
		resp := <-res
		if resp == nil {
			logger.Debugw("RoomOffline response channel closed", "roomName", roomName)
			break
		}

		if resp.Result.RoomName == req.RoomName && resp.Result.NodeId != string(r.localNode) {
			n = append(n, livekit.NodeID(resp.Result.NodeId))
		}
	}

	return &n, nil
}

func (r *cloudRelaySignalClient) StartParticipantRelaySignal(
	ctx context.Context,
	roomName livekit.RoomName,
	toNode livekit.NodeID,
	pri ParticipantRelayInit,
) (
	connectionID livekit.ConnectionID,
	reqSink routing.MessageSink,
	resSource routing.MessageSource,
	err error,
) {
	connectionID = livekit.ConnectionID(guid.New("CO_"))

	srs, err := pri.ToStartRelaySession(roomName, connectionID)
	if err != nil {
		return
	}
	//srs := &rpc.StartRelaySession{
	//	RoomName:     string(roomName),
	//	ConnectionId: string(connectionID),
	//}

	l := logger.GetLogger().WithValues(
		"room", roomName,
		"toNode", toNode,
		"participant", pri.Identity,
		"connID", connectionID,
	)

	l.Debugw("starting cloud relay signal connection")

	stream, err := r.client.RoomSignalRelay(ctx, toNode)
	if err != nil {
		log.Fatalf("failed to start cloud relay signal stream: %v", err)
		//prometheus.MessageCounter.WithLabelValues("cloud_relay_signal", "failure").Add(1)
		return
	}

	err = stream.Send(&rpc.RoomSignalRelayRequest{StartRelaySession: srs})
	if err != nil {
		stream.Close(err)
		log.Fatalf("failed to start cloud relay signal stream: %v", err)
		//prometheus.MessageCounter.WithLabelValues("cloud_relay_signal", "failure").Add(1)
		return
	}

	sink := routing.NewSignalMessageSink(routing.SignalSinkParams[*rpc.RoomSignalRelayRequest, *rpc.RoomSignalRelayResponse]{
		Logger:         l,
		Stream:         stream,
		Config:         r.config,
		Writer:         roomSignalRelayRequestMessageWriter{},
		CloseOnFailure: true,
		BlockOnClose:   true,
		ConnectionID:   connectionID,
	})
	resChan := routing.NewDefaultMessageChannel(connectionID)

	go func() {
		r.active.Inc()
		defer r.active.Dec()

		err := routing.CopySignalStreamToMessageChannel[*rpc.RoomSignalRelayRequest, *rpc.RoomSignalRelayResponse](
			stream,
			resChan,
			roomSignalRelayResponseMessageReader{},
			r.config,
		)
		l.Debugw("could relay signal stream closed", "error", err)

		resChan.Close()
	}()

	return connectionID, sink, resChan, nil
}

type CloudRelaySignalServer struct {
	server   rpc.TypedCloudRelayServiceServer
	nodeID   livekit.NodeID
	roomName livekit.RoomName
}

func NewCloudRelaySignalServer(
	nodeID livekit.NodeID,
	roomName livekit.RoomName,
	//region string,
	bus psrpc.MessageBus,
	config config.SignalRelayConfig,
	sessionHandler RelaySessionHandler,
) (*CloudRelaySignalServer, error) {
	s, err := rpc.NewTypedCloudRelayServiceServer(
		nodeID,
		roomName,
		&CloudRelayService{nodeID, sessionHandler, config},
		bus,
		//middleware.WithServerMetrics(lrpc.PSRPCMetricsObserver{}),
		//psrpc.WithServerChannelSize(config.StreamBufferSize),
	)
	if err != nil {
		return nil, err
	}
	return &CloudRelaySignalServer{s, nodeID, roomName}, nil
}

func NewDefaultCloudRelaySignalServer(
	currentNode routing.LocalNode,
	roomName livekit.RoomName,
	bus psrpc.MessageBus,
	config config.SignalRelayConfig,
	roomManager *service.RoomManager,
) (r *CloudRelaySignalServer, err error) {
	return NewCloudRelaySignalServer(currentNode.NodeID(), roomName, bus, config,
		&defaultRelaySessionHandler{currentNode, roomName, roomManager})
}

func (s *CloudRelaySignalServer) Start() error {
	logger.Debugw("starting relay signal server", "topic", s.nodeID)
	log.Printf("starting relay signal server with topic: %s", s.nodeID)
	return s.server.RegisterAllNodeTopics(s.nodeID)
}

func (s *CloudRelaySignalServer) Stop() {
	s.server.Kill()
}

type roomSignalRelayRequestMessageWriter struct{}

func (e roomSignalRelayRequestMessageWriter) Write(seq uint64, close bool, msgs []proto.Message) *rpc.RoomSignalRelayRequest {
	r := &rpc.RoomSignalRelayRequest{
		Seq:      seq,
		Requests: make([]*livekit.SignalRequest, 0, len(msgs)),
		Close:    close,
	}
	for _, m := range msgs {
		r.Requests = append(r.Requests, m.(*livekit.SignalRequest))
	}
	return r
}

type roomSignalRelayResponseMessageReader struct{}

func (e roomSignalRelayResponseMessageReader) Read(rm *rpc.RoomSignalRelayResponse) ([]proto.Message, error) {
	msgs := make([]proto.Message, 0, len(rm.Responses))
	for _, m := range rm.Responses {
		msgs = append(msgs, m)
	}
	return msgs, nil
}

type roomSignalRelayResponseMessageWriter struct{}

func (e roomSignalRelayResponseMessageWriter) Write(seq uint64, close bool, msgs []proto.Message) *rpc.RoomSignalRelayResponse {
	r := &rpc.RoomSignalRelayResponse{
		Seq:       seq,
		Responses: make([]*livekit.SignalResponse, 0, len(msgs)),
		Close:     close,
	}
	for _, m := range msgs {
		r.Responses = append(r.Responses, m.(*livekit.SignalResponse))
	}
	return r
}

type roomSignalRelayRequestMessageReader struct{}

func (e roomSignalRelayRequestMessageReader) Read(rm *rpc.RoomSignalRelayRequest) ([]proto.Message, error) {
	msgs := make([]proto.Message, 0, len(rm.Requests))
	for _, m := range rm.Requests {
		msgs = append(msgs, m)
	}
	return msgs, nil
}
