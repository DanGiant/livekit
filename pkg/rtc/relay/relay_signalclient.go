package relay

import (
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"
	"sync"
)

type RelaySignalClientParams struct {
	Logger    logger.Logger
	ReqSink   routing.MessageSink
	ResSource routing.MessageSource
}

type RelaySignalClient struct {
	logger          logger.Logger
	reqSink         routing.MessageSink
	resSource       routing.MessageSource
	lock            sync.Mutex
	isStarted       atomic.Bool
	clientState     livekit.ParticipantInfo_State
	pendingResponse *livekit.SignalResponse
	readerClosedCh  chan struct{}
	doneCh          chan struct{}

	HandleJoin              func()
	OnClose                 func()
	OnAnswer                func(sd webrtc.SessionDescription)
	OnOffer                 func(sd webrtc.SessionDescription)
	OnTrickle               func(init webrtc.ICECandidateInit, target livekit.SignalTarget)
	OnParticipantUpdate     func([]*livekit.ParticipantInfo)
	OnLocalTrackPublished   func(response *livekit.TrackPublishedResponse)
	OnSpeakersChanged       func([]*livekit.SpeakerInfo)
	OnConnectionQuality     func([]*livekit.ConnectionQualityInfo)
	OnRoomUpdate            func(room *livekit.Room)
	OnTrackRemoteMuted      func(request *livekit.MuteTrackRequest)
	OnLocalTrackUnpublished func(response *livekit.TrackUnpublishedResponse)
	//OnTokenRefresh          func(refreshToken string)
	OnJoin  func(joinRes *livekit.JoinResponse)
	OnLeave func(*livekit.LeaveRequest)
}

func NewRelaySignalClient(params RelaySignalClientParams) *RelaySignalClient {
	c := &RelaySignalClient{
		logger:      params.Logger,
		reqSink:     params.ReqSink,
		resSource:   params.ResSource,
		clientState: livekit.ParticipantInfo_DISCONNECTED,
	}
	return c
}

func (c *RelaySignalClient) Start() {
	if c.isStarted.Swap(true) {
		return
	}
	c.clientState = livekit.ParticipantInfo_JOINING
	c.readerClosedCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	go c.readWorker(c.readerClosedCh)
}

func (c *RelaySignalClient) Stop() {
	if !c.isStarted.Load() {
		return
	}

	logger.Debugw("sending SignalRequest_Leave for relay participant")

	if !c.reqSink.IsClosed() {
		_ = c.SendRequest(&livekit.SignalRequest{
			Message: &livekit.SignalRequest_Leave{
				Leave: &livekit.LeaveRequest{
					Reason: livekit.DisconnectReason_CLIENT_INITIATED,
					Action: livekit.LeaveRequest_DISCONNECT,
				},
			},
		})
	}

	c.close()
}

func (c *RelaySignalClient) close() {
	c.lock.Lock()
	isStarted := c.IsStarted()
	readerClosedCh := c.readerClosedCh
	doneCh := c.doneCh
	c.lock.Unlock()

	if isStarted && readerClosedCh != nil {
		close(readerClosedCh)
		<-doneCh
	}

	c.reqSink.Close()
	c.resSource.Close()
}

func (c *RelaySignalClient) IsStarted() bool {
	return c.isStarted.Load()
}

func (c *RelaySignalClient) SetState(state livekit.ParticipantInfo_State) {
	c.clientState = state
}

func (c *RelaySignalClient) State() livekit.ParticipantInfo_State {
	return c.clientState
}

func (c *RelaySignalClient) SendICECandidate(candidate webrtc.ICECandidateInit, target livekit.SignalTarget) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Trickle{
			Trickle: ToProtoTrickle(candidate, target),
		},
	})
}

func (c *RelaySignalClient) SendOffer(sd webrtc.SessionDescription) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Offer{
			Offer: ToProtoSessionDescription(sd),
		},
	})
}

func (c *RelaySignalClient) SendAnswer(sd webrtc.SessionDescription) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Answer{
			Answer: ToProtoSessionDescription(sd),
		},
	})
}

func (c *RelaySignalClient) SendMuteTrack(sid string, muted bool) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Mute{
			Mute: &livekit.MuteTrackRequest{
				Sid:   sid,
				Muted: muted,
			},
		},
	})
}

func (c *RelaySignalClient) SendSyncState(state *livekit.SyncState) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_SyncState{
			SyncState: state,
		},
	})
}

func (c *RelaySignalClient) SendLeave() error {
	return c.SendLeaveWithReason(livekit.DisconnectReason_UNKNOWN_REASON)
}

func (c *RelaySignalClient) SendLeaveWithReason(reason livekit.DisconnectReason) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_Leave{
			Leave: &livekit.LeaveRequest{
				Reason: reason,
			},
		},
	})
}

func (c *RelaySignalClient) SendRequest(req *livekit.SignalRequest) error {
	return c.reqSink.WriteMessage(req)
}

func (c *RelaySignalClient) SendUpdateTrackSettings(settings *livekit.UpdateTrackSettings) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_TrackSetting{
			TrackSetting: settings,
		},
	})
}

func (c *RelaySignalClient) SendUpdateParticipantMetadata(metadata *livekit.UpdateParticipantMetadata) error {
	return c.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_UpdateMetadata{
			UpdateMetadata: metadata,
		},
	})
}

func (c *RelaySignalClient) handleResponse(res *livekit.SignalResponse) {
	c.logger.Debugw("handleResponse", "res", res)

	switch msg := res.Message.(type) {
	case *livekit.SignalResponse_Join:
		c.clientState = livekit.ParticipantInfo_JOINED
		c.logger.Debugw("join accepted, awaiting offer", "participant", msg.Join.Participant.Identity)
		if c.OnJoin != nil {
			c.OnJoin(msg.Join)
		}
	case *livekit.SignalResponse_Answer:
		if c.OnAnswer != nil {
			c.OnAnswer(FromProtoSessionDescription(msg.Answer))
		}
	case *livekit.SignalResponse_Offer:
		if c.OnOffer != nil {
			c.OnOffer(FromProtoSessionDescription(msg.Offer))
		}
	case *livekit.SignalResponse_Trickle:
		if c.OnTrickle != nil {
			c.OnTrickle(FromProtoTrickle(msg.Trickle), msg.Trickle.Target)
		}
	case *livekit.SignalResponse_Update:
		if c.OnParticipantUpdate != nil {
			c.OnParticipantUpdate(msg.Update.Participants)
		}
	case *livekit.SignalResponse_SpeakersChanged:
		if c.OnSpeakersChanged != nil {
			c.OnSpeakersChanged(msg.SpeakersChanged.Speakers)
		}
	case *livekit.SignalResponse_TrackPublished:
		if c.OnLocalTrackPublished != nil {
			c.OnLocalTrackPublished(msg.TrackPublished)
		}
	case *livekit.SignalResponse_Mute:
		if c.OnTrackRemoteMuted != nil {
			c.OnTrackRemoteMuted(msg.Mute)
		}
	case *livekit.SignalResponse_ConnectionQuality:
		if c.OnConnectionQuality != nil {
			c.OnConnectionQuality(msg.ConnectionQuality.Updates)
		}
	case *livekit.SignalResponse_RoomUpdate:
		if c.OnRoomUpdate != nil {
			c.OnRoomUpdate(msg.RoomUpdate.Room)
		}
	case *livekit.SignalResponse_Leave:
		c.clientState = livekit.ParticipantInfo_DISCONNECTED
		if c.OnLeave != nil {
			c.OnLeave(msg.Leave)
		}
	//case *livekit.SignalResponse_RefreshToken:
	//	if c.OnTokenRefresh != nil {
	//		c.OnTokenRefresh(msg.RefreshToken)
	//	}
	case *livekit.SignalResponse_TrackUnpublished:
		if c.OnLocalTrackUnpublished != nil {
			c.OnLocalTrackUnpublished(msg.TrackUnpublished)
		}
	}
}

func (c *RelaySignalClient) readWorker(readerClosedCh chan struct{}) {
	defer func() {
		c.isStarted.Store(false)
		if c.OnClose != nil {
			c.OnClose()
		}
		close(c.doneCh)
	}()

	if pending := c.pendingResponse; pending != nil {
		c.handleResponse(pending)
		c.pendingResponse = nil
	}

	for {
		select {
		case <-readerClosedCh:
			return
		case msg := <-c.resSource.ReadChan():
			res, ok := msg.(*livekit.SignalResponse)
			if ok {
				c.handleResponse(res)
			}
		}
	}
}
