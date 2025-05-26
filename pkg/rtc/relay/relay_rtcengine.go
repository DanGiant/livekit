package relay

import (
	"errors"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

const (
	reliableDataChannelName = "_reliable"
	lossyDataChannelName    = "_lossy"

	maxReconnectCount        = 10
	initialReconnectInterval = 300 * time.Millisecond
	maxReconnectInterval     = 60 * time.Second
)

type RelayRTCEngineParams struct {
	logger       logger.Logger
	signalClient *RelaySignalClient
}

type RelayRTCEngine struct {
	params RelayRTCEngineParams
	logger logger.Logger

	pclock    sync.Mutex
	publisher *PCTransport
	client    *RelaySignalClient

	dclock     sync.RWMutex
	reliableDC *webrtc.DataChannel
	lossyDC    *webrtc.DataChannel

	trackPublishedListenersLock sync.Mutex
	trackPublishedListeners     map[string]chan *livekit.TrackPublishedResponse

	hasConnected atomic.Bool
	hasPublish   atomic.Bool
	closed       atomic.Bool

	JoinTimeout time.Duration

	// callbacks
	OnLocalTrackUnpublished func(response *livekit.TrackUnpublishedResponse)
	OnTrackRemoteMuted      func(request *livekit.MuteTrackRequest)
	OnDisconnected          func(reason DisconnectionReason)
	OnMediaTrack            func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver)
	OnParticipantUpdate     func([]*livekit.ParticipantInfo)
	OnSpeakersChanged       func([]*livekit.SpeakerInfo)
	OnDataReceived          func(userPacket *livekit.UserPacket) // Deprecated: Use OnDataPacket instead
	OnDataPacket            func(identity string, dataPacket livekit.DataPacket)
	OnConnectionQuality     func([]*livekit.ConnectionQualityInfo)
	OnRoomUpdate            func(room *livekit.Room)
	OnRestarting            func()
	OnRestarted             func(*livekit.JoinResponse)
	OnResuming              func()
	OnResumed               func()
	OnTranscription         func(*livekit.Transcription)
	OnSignalClientConnected func(*livekit.JoinResponse)
	OnStreamHeader          func(*livekit.DataStream_Header, string)
	OnStreamChunk           func(*livekit.DataStream_Chunk)
	OnStreamTrailer         func(*livekit.DataStream_Trailer)

	onClose     []func()
	onCloseLock sync.Mutex
}

func NewRelayRTCEngine(params RelayRTCEngineParams) *RelayRTCEngine {
	e := &RelayRTCEngine{
		params:                  params,
		logger:                  params.logger,
		client:                  params.signalClient,
		trackPublishedListeners: make(map[string]chan *livekit.TrackPublishedResponse),
		JoinTimeout:             15 * time.Second,
	}

	e.client.OnParticipantUpdate = func(info []*livekit.ParticipantInfo) {
		if f := e.OnParticipantUpdate; f != nil {
			f(info)
		}
	}
	e.client.OnSpeakersChanged = func(si []*livekit.SpeakerInfo) {
		if f := e.OnSpeakersChanged; f != nil {
			f(si)
		}
	}
	e.client.OnLocalTrackPublished = e.handleLocalTrackPublished
	e.client.OnLocalTrackUnpublished = e.handleLocalTrackUnpublished
	e.client.OnTrackRemoteMuted = e.handleTrackRemoteMuted
	e.client.OnConnectionQuality = func(cqi []*livekit.ConnectionQualityInfo) {
		if f := e.OnConnectionQuality; f != nil {
			f(cqi)
		}
	}
	e.client.OnRoomUpdate = func(room *livekit.Room) {
		if f := e.OnRoomUpdate; f != nil {
			f(room)
		}
	}
	e.client.OnLeave = e.handleLeave
	//e.client.OnTokenRefresh = func(refreshToken string) {
	//	e.token.Store(refreshToken)
	//}
	e.client.OnClose = func() { e.handleDisconnect(false) }
	e.onClose = []func(){}
	return e
}

func (e *RelayRTCEngine) AddOnClose(onClose func()) {
	e.onCloseLock.Lock()
	e.onClose = append(e.onClose, onClose)
	e.onCloseLock.Unlock()
}

func (e *RelayRTCEngine) Close() {
	if !e.closed.CompareAndSwap(false, true) {
		return
	}

	go func() {
		//for e.reconnecting.Load() {
		//	time.Sleep(50 * time.Millisecond)
		//}

		e.onCloseLock.Lock()
		onClose := e.onClose
		e.onClose = []func(){}
		e.onCloseLock.Unlock()

		for _, onCloseHandler := range onClose {
			onCloseHandler()
		}

		if publisher, ok := e.Publisher(); ok {
			_ = publisher.Close()
		}
		//if subscriber, ok := e.Subscriber(); ok {
		//	_ = subscriber.Close()
		//}

		//e.client.Stop()
	}()
}

func (e *RelayRTCEngine) IsConnected() bool {
	e.pclock.Lock()
	defer e.pclock.Unlock()

	if e.publisher == nil /*|| e.subscriber == nil*/ {
		return false
	}
	//if e.subscriberPrimary {
	//	return e.subscriber.IsConnected()
	//}
	return e.publisher.IsConnected()
}

func (e *RelayRTCEngine) Publisher() (*PCTransport, bool) {
	e.pclock.Lock()
	defer e.pclock.Unlock()
	return e.publisher, e.publisher != nil
}

func (e *RelayRTCEngine) makeRTCConfiguration(iceServers []*livekit.ICEServer /*, clientConfig *livekit.ClientConfiguration*/) webrtc.Configuration {
	rtcICEServers := FromProtoIceServers(iceServers)
	configuration := webrtc.Configuration{
		ICEServers:         rtcICEServers,
		ICETransportPolicy: webrtc.ICETransportPolicyAll, // e.connParams.ICETransportPolicy,
	}
	//if clientConfig != nil &&
	//	clientConfig.GetForceRelay() == livekit.ClientConfigSetting_ENABLED {
	//	configuration.ICETransportPolicy = webrtc.ICETransportPolicyRelay
	//}
	return configuration
}

func (e *RelayRTCEngine) configure(
	configuration webrtc.Configuration,
	// iceServers []*livekit.ICEServer,
	// clientConfig *livekit.ClientConfiguration,
	// subscriberPrimary *bool,
) error {

	//configuration := e.makeRTCConfiguration(iceServers /*, clientConfig*/)
	e.pclock.Lock()
	defer e.pclock.Unlock()

	// remove previous transport
	if e.publisher != nil {
		e.publisher.Close()
		e.publisher = nil
	}
	//if e.subscriber != nil {
	//	e.subscriber.Close()
	//	e.subscriber = nil
	//}

	var err error
	if e.publisher, err = NewPCTransport(PCTransportParams{
		Configuration: configuration,
		//RetransmitBufferSize: e.connParams.RetransmitBufferSize,
		//Pacer:                e.connParams.Pacer,
		//Interceptors:         e.connParams.Interceptors,
		//OnRTTUpdate:          e.setRTT,
		IsSender: true,
	}); err != nil {
		return err
	}
	//if e.subscriber, err = NewPCTransport(PCTransportParams{
	//	Configuration:        configuration,
	//	RetransmitBufferSize: e.connParams.RetransmitBufferSize,
	//}); err != nil {
	//	return err
	//}
	e.publisher.SetLogger(e.logger)
	//e.subscriber.SetLogger(e.logger)
	//e.logger.Debugw("Using ICE servers", "servers", iceServers)

	//if subscriberPrimary != nil {
	//	e.subscriberPrimary = *subscriberPrimary
	//}
	//e.subscriber.OnRemoteDescriptionSettled(e.createPublisherAnswerAndSend)

	e.publisher.pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			// done
			return
		}
		init := candidate.ToJSON()
		e.logger.Debugw("local ICE candidate",
			"target", livekit.SignalTarget_PUBLISHER,
			"candidate", init.Candidate,
		)
		if err := e.client.SendICECandidate(init, livekit.SignalTarget_PUBLISHER); err != nil {
			e.logger.Errorw("could not send ICE candidates for publisher", err)
		}

	})

	primaryTransport := e.publisher
	primaryTransport.pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		switch state {
		case webrtc.ICEConnectionStateConnected:
			var fields []interface{}
			if pair, err := primaryTransport.GetSelectedCandidatePair(); err == nil {
				fields = append(fields, "iceCandidatePair", pair)
			}
			e.logger.Debugw("ICE connected", fields...)
		case webrtc.ICEConnectionStateDisconnected:
			e.logger.Debugw("ICE disconnected")
		case webrtc.ICEConnectionStateFailed:
			e.logger.Debugw("ICE failed")
			e.handleDisconnect(false)
		}
	})

	e.publisher.OnOffer = func(offer webrtc.SessionDescription) {
		e.hasPublish.Store(true)
		e.logger.Debugw("send offer for publisher", "offer", offer)
		if err := e.client.SendOffer(offer); err != nil {
			e.logger.Errorw("could not send offer", err)
		}
	}

	trueVal := true
	maxRetries := uint16(1)
	e.dclock.Lock()
	e.lossyDC, err = e.publisher.PeerConnection().CreateDataChannel(lossyDataChannelName, &webrtc.DataChannelInit{
		Ordered:        &trueVal,
		MaxRetransmits: &maxRetries,
	})
	if err != nil {
		e.dclock.Unlock()
		return err
	}
	e.lossyDC.OnMessage(e.handleDataPacket)
	e.reliableDC, err = e.publisher.PeerConnection().CreateDataChannel(reliableDataChannelName, &webrtc.DataChannelInit{
		Ordered: &trueVal,
	})
	if err != nil {
		e.dclock.Unlock()
		return err
	}
	e.reliableDC.OnMessage(e.handleDataPacket)
	e.dclock.Unlock()

	// configure client
	e.client.OnJoin = func(join *livekit.JoinResponse) {
		e.logger.Debugw("relay rtc engine onJoin")

		var iceServers []*livekit.ICEServer
		if join.IceServers != nil && len(join.IceServers) > 0 {
			for _, iceServer := range join.IceServers {
				iceServers = append(iceServers, iceServer)
			}
		} else {
			iceServers := make([]*livekit.ICEServer, 1)
			iceServers[0] = &livekit.ICEServer{
				Urls: []string{"stun:stun.l.google.com:19302"},
			}
		}
		configuration := e.makeRTCConfiguration(iceServers)
		err := e.publisher.SetConfiguration(configuration)
		if err != nil {
			e.logger.Errorw("set configuration for publisher failed", err)
		}

		e.publisher.Negotiate()

		if e.client.HandleJoin != nil {
			e.client.HandleJoin()
		}
	}

	e.client.OnAnswer = func(sd webrtc.SessionDescription) {
		if e.closed.Load() {
			e.logger.Debugw("ignoring SDP answer after closed")
			return
		}

		e.logger.Debugw("received answer for publisher", "answer", sd)
		if err := e.publisher.SetRemoteDescription(sd); err != nil {
			e.logger.Errorw("could not set remote description", err)
		} else {
			e.logger.Debugw("successfully set publisher answer")
		}
	}

	e.client.OnTrickle = func(init webrtc.ICECandidateInit, target livekit.SignalTarget) {
		if e.closed.Load() {
			e.logger.Debugw("ignoring trickle after closed")
			return
		}

		var err error
		e.logger.Debugw("remote ICE candidate", "target", target, "candidate", init.Candidate)
		if target == livekit.SignalTarget_PUBLISHER {
			err = e.publisher.AddICECandidate(init)
		} else if target == livekit.SignalTarget_SUBSCRIBER {
			e.logger.Infow("do not support subscriber")
			//err = e.subscriber.AddICECandidate(init)
		}
		if err != nil {
			e.logger.Errorw("could not add ICE candidate", err)
		}
	}

	e.client.OnOffer = func(sd webrtc.SessionDescription) {
		if e.closed.Load() {
			e.logger.Debugw("ignoring SDP offer after closed")
			return
		}

		e.logger.Infow("received offer for subscriber, do not support subscriber", "sdp", sd)
		//if err := e.subscriber.SetRemoteDescription(sd); err != nil {
		//	e.logger.Errorw("could not set remote description", err)
		//	return
		//}
	}
	return nil
}

func (e *RelayRTCEngine) GetDataChannel(kind livekit.DataPacket_Kind) *webrtc.DataChannel {
	e.dclock.RLock()
	defer e.dclock.RUnlock()
	if kind == livekit.DataPacket_RELIABLE {
		return e.reliableDC
	}
	return e.lossyDC
}

//func (e *RelayRTCEngine) GetDataChannelSub(kind livekit.DataPacket_Kind) *webrtc.DataChannel {
//	e.dclock.RLock()
//	defer e.dclock.RUnlock()
//	if kind == livekit.DataPacket_RELIABLE {
//		return e.reliableDCSub
//	}
//	return e.lossyDCSub
//}

func waitUntilConnected(d time.Duration, test func() bool) error {
	if test() {
		return nil
	}

	timeout := time.NewTimer(d)
	defer timeout.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timeout.C:
			return ErrConnectionTimeout
		case <-ticker.C:
			if test() {
				return nil
			}
		}
	}
}

func (e *RelayRTCEngine) waitUntilConnected() error {
	return waitUntilConnected(e.JoinTimeout, func() bool {
		if e.IsConnected() {
			//e.requiresFullReconnect.Store(false)
			return true
		}
		return false
	})
}

func (e *RelayRTCEngine) ensurePublisherConnected(ensureDataReady bool) error {
	//e.pclock.Lock()
	//subscriberPrimary := e.subscriberPrimary
	//e.pclock.Unlock()
	//if !subscriberPrimary {
	//	return e.waitUntilConnected()
	//}

	var negotiated bool
	return waitUntilConnected(e.JoinTimeout, func() bool {
		if publisher, ok := e.Publisher(); ok {
			if publisher.IsConnected() && (!ensureDataReady || e.dataPubChannelReady()) {
				return true
			}
			if !negotiated {
				publisher.Negotiate()
				negotiated = true
			}
		}
		return false
	})
}

func (e *RelayRTCEngine) dataPubChannelReady() bool {
	e.dclock.RLock()
	defer e.dclock.RUnlock()
	return e.reliableDC.ReadyState() == webrtc.DataChannelStateOpen && e.lossyDC.ReadyState() == webrtc.DataChannelStateOpen
}

func (e *RelayRTCEngine) RegisterTrackPublishedListener(cid string, c chan *livekit.TrackPublishedResponse) {
	e.trackPublishedListenersLock.Lock()
	e.trackPublishedListeners[cid] = c
	e.trackPublishedListenersLock.Unlock()
}

func (e *RelayRTCEngine) UnregisterTrackPublishedListener(cid string) {
	e.trackPublishedListenersLock.Lock()
	delete(e.trackPublishedListeners, cid)
	e.trackPublishedListenersLock.Unlock()
}

func (e *RelayRTCEngine) handleLocalTrackPublished(res *livekit.TrackPublishedResponse) {
	e.trackPublishedListenersLock.Lock()
	listener, ok := e.trackPublishedListeners[res.Cid]
	e.trackPublishedListenersLock.Unlock()

	if ok {
		listener <- res
	}
}

func (e *RelayRTCEngine) handleLocalTrackUnpublished(res *livekit.TrackUnpublishedResponse) {
	if e.OnLocalTrackUnpublished != nil {
		e.OnLocalTrackUnpublished(res)
	}
}

func (e *RelayRTCEngine) handleTrackRemoteMuted(request *livekit.MuteTrackRequest) {
	if e.OnTrackRemoteMuted != nil {
		e.OnTrackRemoteMuted(request)
	}
}

func (e *RelayRTCEngine) handleDataPacket(msg webrtc.DataChannelMessage) {
	packet, err := e.readDataPacket(msg)
	if err != nil {
		return
	}
	identity := packet.ParticipantIdentity
	switch msg := packet.Value.(type) {
	case *livekit.DataPacket_User:
		m := msg.User
		//lint:ignore SA1019 backward compatibility
		if ptr := &m.ParticipantIdentity; *ptr == "" {
			*ptr = identity
		}
		//lint:ignore SA1019 backward compatibility
		if ptr := &m.DestinationIdentities; len(*ptr) == 0 {
			*ptr = packet.DestinationIdentities
		}
		if onDataReceived := e.OnDataReceived; onDataReceived != nil {
			onDataReceived(m)
		}
		if e.OnDataPacket != nil {
			if identity == "" {
				//lint:ignore SA1019 backward compatibility
				identity = m.ParticipantIdentity
			}
			userDataPacket := &UserDataPacket{
				Payload: m.Payload,
				Topic:   m.GetTopic(),
			}
			e.OnDataPacket(identity, *userDataPacket.ToProto())
		}
	//case *livekit.DataPacket_SipDtmf:
	//	if e.OnDataPacket != nil {
	//		e.OnDataPacket(identity, msg.SipDtmf)
	//	}
	case *livekit.DataPacket_Transcription:
		if e.OnTranscription != nil {
			e.OnTranscription(msg.Transcription)
		}
	//case *livekit.DataPacket_RpcRequest:
	//	if e.OnRpcRequest != nil {
	//		e.OnRpcRequest(packet.ParticipantIdentity, msg.RpcRequest.Id, msg.RpcRequest.Method, msg.RpcRequest.Payload, time.Duration(msg.RpcRequest.ResponseTimeoutMs)*time.Millisecond, msg.RpcRequest.Version)
	//	}
	//case *livekit.DataPacket_RpcAck:
	//	if e.OnRpcAck != nil {
	//		e.OnRpcAck(msg.RpcAck.RequestId)
	//	}
	//case *livekit.DataPacket_RpcResponse:
	//	if e.OnRpcResponse != nil {
	//		switch res := msg.RpcResponse.Value.(type) {
	//		case *livekit.RpcResponse_Payload:
	//			e.OnRpcResponse(msg.RpcResponse.RequestId, &res.Payload, nil)
	//		case *livekit.RpcResponse_Error:
	//			e.OnRpcResponse(msg.RpcResponse.RequestId, nil, fromProto(res.Error))
	//		}
	//	}
	case *livekit.DataPacket_StreamHeader:
		if e.OnStreamHeader != nil {
			e.OnStreamHeader(msg.StreamHeader, identity)
		}
	case *livekit.DataPacket_StreamChunk:
		if e.OnStreamChunk != nil {
			e.OnStreamChunk(msg.StreamChunk)
		}
	case *livekit.DataPacket_StreamTrailer:
		if e.OnStreamTrailer != nil {
			e.OnStreamTrailer(msg.StreamTrailer)
		}
	}
}

func (e *RelayRTCEngine) readDataPacket(msg webrtc.DataChannelMessage) (*livekit.DataPacket, error) {
	dataPacket := &livekit.DataPacket{}
	if msg.IsString {
		err := protojson.Unmarshal(msg.Data, dataPacket)
		return dataPacket, err
	}
	err := proto.Unmarshal(msg.Data, dataPacket)
	return dataPacket, err
}

func (e *RelayRTCEngine) handleDisconnect(fullReconnect bool) {
	// do not retry until fully connected
	if e.closed.Load() || !e.hasConnected.Load() {
		return
	}

	//if !e.reconnecting.CompareAndSwap(false, true) {
	//	if fullReconnect {
	//		e.requiresFullReconnect.Store(true)
	//	}
	//	return
	//}

	//go func() {
	//	//defer e.reconnecting.Store(false)
	//	for reconnectCount := 0; reconnectCount < maxReconnectCount && !e.closed.Load(); reconnectCount++ {
	//		if e.requiresFullReconnect.Load() {
	//			fullReconnect = true
	//		}
	//		if fullReconnect {
	//			if reconnectCount == 0 && e.OnRestarting != nil {
	//				e.OnRestarting()
	//			}
	//			e.logger.Infow("restarting connection...", "reconnectCount", reconnectCount)
	//			if err := e.restartConnection(); err != nil {
	//				e.logger.Errorw("restart connection failed", err)
	//			} else {
	//				return
	//			}
	//		} else {
	//			if reconnectCount == 0 && e.OnResuming != nil {
	//				e.OnResuming()
	//			}
	//			e.logger.Infow("resuming connection...", "reconnectCount", reconnectCount)
	//			if err := e.resumeConnection(); err != nil {
	//				e.logger.Errorw("resume connection failed", err)
	//			} else {
	//				return
	//			}
	//		}
	//
	//		delay := time.Duration(reconnectCount*reconnectCount) * initialReconnectInterval
	//		if delay > maxReconnectInterval {
	//			break
	//		}
	//		if reconnectCount < maxReconnectCount-1 {
	//			time.Sleep(delay)
	//		}
	//	}
	//
	//	if e.OnDisconnected != nil {
	//		e.OnDisconnected(Failed)
	//	}
	//}()
}

//func (e *RelayRTCEngine) resumeConnection() error {
//	reconnect, err := e.client.Reconnect(e.url, e.token.Load(), *e.connParams, e.CbGetLocalParticipantSID())
//	if err != nil {
//		return err
//	}
//
//	if reconnect != nil {
//		configuration := e.makeRTCConfiguration(reconnect.IceServers, reconnect.ClientConfiguration)
//		e.pclock.Lock()
//		if err = e.publisher.SetConfiguration(configuration); err != nil {
//			logger.Errorw("could not set rtc configuration for publisher", err)
//			e.pclock.Unlock()
//			return err
//		}
//		if err = e.subscriber.SetConfiguration(configuration); err != nil {
//			logger.Errorw("could not set rtc configuration for subscriber", err)
//			e.pclock.Unlock()
//			return err
//		}
//		e.pclock.Unlock()
//	}
//	e.client.Start()
//
//	// send offer if publisher enabled
//	e.pclock.Lock()
//	sendOffer := !e.subscriberPrimary || e.hasPublish.Load()
//	publisher := e.publisher
//	e.pclock.Unlock()
//	if sendOffer {
//		if err := publisher.createAndSendOffer(&webrtc.OfferOptions{
//			ICERestart: true,
//		}); err != nil {
//			return err
//		}
//	}
//
//	if err = e.waitUntilConnected(); err != nil {
//		return err
//	}
//
//	if e.OnResumed != nil {
//		e.OnResumed()
//	}
//	return nil
//}

//func (e *RelayRTCEngine) restartConnection() error {
//	if e.client.IsStarted() {
//		// TODO: special reason for reconnect?
//		e.client.SendLeaveWithReason(livekit.DisconnectReason_UNKNOWN_REASON)
//	}
//	e.client.Close()
//
//	res, err := e.Join(e.url, e.token.Load(), e.connParams)
//	if err != nil {
//		return err
//	}
//
//	if e.OnRestarted != nil {
//		e.OnRestarted(res)
//	}
//	return nil
//}

//func (e *RelayRTCEngine) createPublisherAnswerAndSend() error {
//	answer, err := e.subscriber.pc.CreateAnswer(nil)
//	if err != nil {
//		e.logger.Errorw("could not create answer", err)
//		return err
//	}
//	if err := e.subscriber.pc.SetLocalDescription(answer); err != nil {
//		e.logger.Errorw("could not set subscriber local description", err)
//		return err
//	}
//	if err := e.client.SendAnswer(answer); err != nil {
//		e.logger.Errorw("could not send answer for subscriber", err)
//		return err
//	}
//	return nil
//}

func (e *RelayRTCEngine) handleLeave(leave *livekit.LeaveRequest) {
	//if leave.GetCanReconnect() {
	//	e.handleDisconnect(true)
	//} else {
	reason := leave.GetReason()
	e.logger.Infow("server initiated leave",
		"reason", reason,
		"canReconnect", leave.GetCanReconnect(),
	)
	if e.OnDisconnected != nil {
		// TODO: migrate to LeaveRequest.Action
		e.OnDisconnected(GetDisconnectionReason(reason))
	}
	//}
}

func (e *RelayRTCEngine) publishDataPacket(pck *livekit.DataPacket, kind livekit.DataPacket_Kind) error {
	data, err := proto.Marshal(pck)
	if err != nil {
		e.logger.Errorw("could not marshal data packet", err)
		return err
	}

	err = e.ensurePublisherConnected(true)
	if err != nil {
		e.logger.Errorw("could not ensure publisher connected", err)
		return err
	}

	dc := e.GetDataChannel(kind)
	if dc == nil {
		e.logger.Errorw("could not get data channel", nil, "kind", kind)
		return errors.New("datachannel not found")
	}

	return dc.Send(data)
}

func (e *RelayRTCEngine) publishDataPacketReliable(pck *livekit.DataPacket) error {
	return e.publishDataPacket(pck, livekit.DataPacket_RELIABLE)
}

//lint:ignore U1000 Ignore unused function
func (e *RelayRTCEngine) publishDataPacketLossy(pck *livekit.DataPacket) error {
	return e.publishDataPacket(pck, livekit.DataPacket_LOSSY)
}

func (e *RelayRTCEngine) publishStreamHeader(header *livekit.DataStream_Header, destinationIdentities []string) error {
	packet := &livekit.DataPacket{
		DestinationIdentities: destinationIdentities,
		Value: &livekit.DataPacket_StreamHeader{
			StreamHeader: header,
		},
	}

	publishErr := e.publishDataPacketReliable(packet)
	if publishErr != nil {
		e.logger.Errorw("could not publish stream header", publishErr)
	}
	return publishErr
}

func (e *RelayRTCEngine) publishStreamChunk(chunk *livekit.DataStream_Chunk, destinationIdentities []string) error {
	packet := &livekit.DataPacket{
		DestinationIdentities: destinationIdentities,
		Value: &livekit.DataPacket_StreamChunk{
			StreamChunk: chunk,
		},
	}

	publishErr := e.publishDataPacketReliable(packet)
	if publishErr != nil {
		e.logger.Errorw("could not publish stream chunk", publishErr)
	}
	return publishErr
}

func (e *RelayRTCEngine) publishStreamTrailer(streamId string, destinationIdentities []string) error {
	packet := &livekit.DataPacket{
		DestinationIdentities: destinationIdentities,
		Value: &livekit.DataPacket_StreamTrailer{
			StreamTrailer: &livekit.DataStream_Trailer{
				StreamId: streamId,
			},
		},
	}

	publishErr := e.publishDataPacketReliable(packet)
	if publishErr != nil {
		e.logger.Errorw("could not publish stream trailer", publishErr)
	}
	return publishErr
}

func (e *RelayRTCEngine) isBufferStatusLow(kind livekit.DataPacket_Kind) bool {
	dc := e.GetDataChannel(kind)
	if dc != nil {
		return dc.BufferedAmount() <= dc.BufferedAmountLowThreshold()
	}
	return false
}

func (e *RelayRTCEngine) waitForBufferStatusLow(kind livekit.DataPacket_Kind) {
	for !e.isBufferStatusLow(kind) {
		time.Sleep(10 * time.Millisecond)
	}
}
