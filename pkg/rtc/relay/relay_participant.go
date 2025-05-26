package relay

import (
	"github.com/google/uuid"
	"github.com/livekit/protocol/logger"
	"github.com/pion/webrtc/v4"
	"github.com/pkg/errors"
	"maps"
	"mime"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/livekit/protocol/livekit"
)

const (
	trackPublishTimeout = 10 * time.Second
)

var (
	relayClientInterval = 3 * time.Second
)

type ClientParticipant interface {
	SID() string
	Identity() string
	Name() string
	IsSpeaking() bool
	AudioLevel() float32
	TrackPublications() []TrackPublication
	IsCameraEnabled() bool
	IsMicrophoneEnabled() bool
	IsScreenShareEnabled() bool
	IsScreenShareAudioEnabled() bool
	Metadata() string
	Attributes() map[string]string
	GetTrackPublication(source livekit.TrackSource) TrackPublication
	Permissions() *livekit.ParticipantPermission
	Close()

	setAudioLevel(level float32)
	setIsSpeaking(speaking bool)
	setConnectionQualityInfo(info *livekit.ConnectionQualityInfo)
}

//-------------------------------------------------------

type baseParticipant struct {
	sid               string
	identity          string
	name              string
	audioLevel        atomic.Float64
	metadata          string
	attributes        map[string]string
	isSpeaking        atomic.Bool
	info              *livekit.ParticipantInfo
	connectionQuality *livekit.ConnectionQualityInfo
	lock              sync.RWMutex

	Callback *ParticipantCallback
	//roomCallback *RoomCallback

	audioTracks *sync.Map
	videoTracks *sync.Map
	tracks      *sync.Map
}

func newBaseParticipant( /*roomCallback *RoomCallback*/ ) *baseParticipant {
	p := &baseParticipant{
		audioTracks: &sync.Map{},
		videoTracks: &sync.Map{},
		tracks:      &sync.Map{},
		//roomCallback: roomCallback,
		Callback: NewParticipantCallback(),
	}
	// need to initialize
	p.setAudioLevel(0)
	p.setIsSpeaking(false)
	return p
}

func (p *baseParticipant) SID() string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.sid
}

func (p *baseParticipant) Identity() string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.identity
}

func (p *baseParticipant) Name() string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.name
}

func (p *baseParticipant) Metadata() string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.metadata
}

func (p *baseParticipant) Attributes() map[string]string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return maps.Clone(p.attributes)
}

func (p *baseParticipant) Permissions() *livekit.ParticipantPermission {
	p.lock.RLock()
	defer p.lock.RUnlock()
	perm := p.info.GetPermission()
	if perm != nil {
		return proto.Clone(perm).(*livekit.ParticipantPermission)
	}
	return nil
}

func (p *baseParticipant) Close() {

}

func (p *baseParticipant) IsSpeaking() bool {
	return p.isSpeaking.Load()
}

func (p *baseParticipant) AudioLevel() float32 {
	return float32(p.audioLevel.Load())
}

func (p *baseParticipant) TrackPublications() []TrackPublication {
	tracks := make([]TrackPublication, 0)
	p.tracks.Range(func(_, value interface{}) bool {
		track := value.(TrackPublication)
		tracks = append(tracks, track)
		return true
	})
	return tracks
}

func (p *baseParticipant) GetTrackPublication(source livekit.TrackSource) TrackPublication {
	var pub TrackPublication
	p.tracks.Range(func(_, value interface{}) bool {
		trackPub := value.(TrackPublication)
		if trackPub.Source() == source {
			pub = trackPub
			return false
		}
		return true
	})
	return pub
}

func (p *baseParticipant) IsCameraEnabled() bool {
	pub := p.GetTrackPublication(livekit.TrackSource_CAMERA)
	return pub != nil && !pub.IsMuted()
}

func (p *baseParticipant) IsMicrophoneEnabled() bool {
	pub := p.GetTrackPublication(livekit.TrackSource_MICROPHONE)
	return pub != nil && !pub.IsMuted()
}

func (p *baseParticipant) IsScreenShareEnabled() bool {
	pub := p.GetTrackPublication(livekit.TrackSource_SCREEN_SHARE)
	return pub != nil && !pub.IsMuted()
}

func (p *baseParticipant) IsScreenShareAudioEnabled() bool {
	pub := p.GetTrackPublication(livekit.TrackSource_SCREEN_SHARE_AUDIO)
	return pub != nil && !pub.IsMuted()
}

func (p *baseParticipant) setAudioLevel(level float32) {
	p.audioLevel.Store(float64(level))
}

func (p *baseParticipant) setIsSpeaking(speaking bool) {
	if p.isSpeaking.Swap(speaking) == speaking {
		return
	}
	p.Callback.OnIsSpeakingChanged(p)
	//p.roomCallback.OnIsSpeakingChanged(p)
}

func (p *baseParticipant) setConnectionQualityInfo(info *livekit.ConnectionQualityInfo) {
	p.lock.Lock()
	p.connectionQuality = info
	p.lock.Unlock()
	p.Callback.OnConnectionQualityChanged(info, p)
	//p.roomCallback.OnConnectionQualityChanged(info, p)
}

func attributeChanges(old, cur map[string]string) map[string]string {
	diff := make(map[string]string)
	for k, v := range cur {
		if old[k] != v {
			diff[k] = v // added or changed
		}
	}
	for k := range old {
		if _, ok := cur[k]; !ok {
			diff[k] = "" // deleted
		}
	}
	return diff
}

func (p *baseParticipant) updateInfo(pi *livekit.ParticipantInfo, participant ClientParticipant) bool {
	p.lock.Lock()
	if p.info != nil && p.info.Version > pi.Version {
		// already updated with a later version
		p.lock.Unlock()
		return false
	}
	p.info = pi
	p.identity = pi.Identity
	p.sid = pi.Sid
	p.name = pi.Name
	oldMetadata := p.metadata
	p.metadata = pi.Metadata
	metaChanged := oldMetadata != p.metadata
	attrsChanges := attributeChanges(p.attributes, pi.Attributes)
	p.attributes = maps.Clone(pi.Attributes)
	p.lock.Unlock()

	if metaChanged {
		p.Callback.OnMetadataChanged(oldMetadata, participant)
		//p.roomCallback.OnMetadataChanged(oldMetadata, participant)
	}
	if len(attrsChanges) != 0 {
		p.Callback.OnAttributesChanged(attrsChanges, participant)
		//p.roomCallback.OnAttributesChanged(attrsChanges, participant)
	}
	return true
}

func (p *baseParticipant) addPublication(publication TrackPublication) {
	sid := publication.SID()
	p.tracks.Store(sid, publication)
	switch publication.Kind() {
	case TrackKindAudio:
		p.audioTracks.Store(sid, publication)
	case TrackKindVideo:
		p.videoTracks.Store(sid, publication)
	}
}

func (p *baseParticipant) getPublication(sid string) TrackPublication {
	p.lock.Lock()
	defer p.lock.Unlock()

	track, ok := p.tracks.Load(sid)
	if !ok {
		return nil
	}
	return track.(TrackPublication)
}

//-------------------------------------------------------

type RelayParticipantParams struct {
	Logger        logger.Logger
	Configuration webrtc.Configuration
	RoomName      livekit.RoomName
	NodeID        livekit.NodeID
	ServerInfo    *livekit.ServerInfo
	SignalClient  *RelaySignalClient
}

type RelayParticipant struct {
	baseParticipant

	params                 RelayParticipantParams
	logger                 logger.Logger
	roomName               livekit.RoomName
	nodeID                 livekit.NodeID
	subscriptionPermission *livekit.SubscriptionPermission
	serverInfo             *livekit.ServerInfo
	closeCh                chan struct{}
	doneCh                 chan struct{}

	client *RelaySignalClient
	engine *RelayRTCEngine

	onClose []func()
}

func NewRelayParticipant(params RelayParticipantParams) *RelayParticipant {
	rtcEngine := NewRelayRTCEngine(RelayRTCEngineParams{
		logger:       params.Logger,
		signalClient: params.SignalClient,
	})

	p := &RelayParticipant{
		baseParticipant: *newBaseParticipant( /*roomcallback*/ ),
		params:          params,
		logger:          params.Logger,
		roomName:        params.RoomName,
		nodeID:          params.NodeID,
		client:          params.SignalClient,
		engine:          rtcEngine,
		serverInfo:      params.ServerInfo,
		closeCh:         make(chan struct{}),
		doneCh:          make(chan struct{}),
		onClose:         make([]func(), 0),

		subscriptionPermission: &livekit.SubscriptionPermission{
			AllParticipants: true,
		},
	}

	return p
}

func (m *RelayParticipant) GetLogger() logger.Logger {
	return m.logger
}

func (m *RelayParticipant) DestNodeID() livekit.NodeID {
	return m.nodeID
}

func (m *RelayParticipant) AddOnClose(f func()) {
	if f == nil {
		return
	}

	m.lock.Lock()
	m.onClose = append(m.onClose, f)
	m.lock.Unlock()
}

func (m *RelayParticipant) IsClosed() bool {
	select {
	case <-m.closeCh:
		return true
	default:
		return false
	}
}

func (p *RelayParticipant) Close() {
	if p.IsClosed() {
		p.logger.Debugw("relay participant already closed!", "destNodeID", p.DestNodeID(),
			"Name", p.Name())
		return
	}

	p.logger.Debugw("closing relay participant", "destNodeID", p.DestNodeID(),
		"Name", p.Name())

	p.lock.Lock()
	close(p.closeCh)
	p.lock.Unlock()

	// wait for relayClientWorker routine done
	<-p.doneCh

	// close all relay tracks
	p.client.Stop()
	p.engine.Close()

	p.lock.Lock()
	onclose := p.onClose
	p.onClose = make([]func(), 0)
	p.lock.Unlock()

	// notify all registered onClose
	for _, f := range onclose {
		f()
	}
}

func (p *RelayParticipant) Start() error {
	iceServers := make([]*livekit.ICEServer, 1)
	iceServers[0] = &livekit.ICEServer{
		Urls: []string{"stun:stun.l.google.com:19302"},
	}
	err := p.engine.configure(p.params.Configuration)
	if err != nil {
		return errors.Wrap(err, "failed to configure IRC")
	}

	p.engine.OnDisconnected = func(reason DisconnectionReason) {
		p.logger.Debugw("closing relay participant", "destNodeID", p.DestNodeID(),
			"Name", p.Name())
		if !p.IsClosed() {
			go p.Close()
		}
	}

	p.client.Start()

	go p.relayClientWorker()
	return nil
}

func (p *RelayParticipant) Stop() {
	p.client.Stop()
}

func (p *RelayParticipant) State() livekit.ParticipantInfo_State {
	return p.client.State()
}

func (p *RelayParticipant) relayClientWorker() {
	reconcileTicker := time.NewTicker(relayClientInterval)
	defer reconcileTicker.Stop()
	defer close(p.doneCh)

	for {
		select {
		case <-p.closeCh:
			return
		case <-reconcileTicker.C:
			if p.client.reqSink.IsClosed() && !p.IsClosed() {
				p.logger.Debugw("relay signal channel closed, closing relay participant",
					"destNodeID", p.DestNodeID(), "Name", p.baseParticipant.Name())
				go p.Close()
				return
			}

			reqMessageIn := &livekit.SignalRequest{
				Message: &livekit.SignalRequest_PingReq{PingReq: &livekit.Ping{
					Timestamp: time.Now().UnixMilli(),
				}},
			}
			err := p.client.reqSink.WriteMessage(reqMessageIn)
			if err != nil {
				p.logger.Errorw("send heart beat ping failed", err,
					"RemoteNode", string(p.nodeID), "Room", p.roomName, "Participant", string(p.Identity()))

				if !p.IsClosed() {
					p.logger.Debugw("closing relay participant", "destNodeID", p.DestNodeID(),
						"Name", p.Name())
					go p.Close()
				}
				return
			}
		}
	}
}

func (p *RelayParticipant) PublishTrack(track webrtc.TrackLocal, opts *TrackPublicationOptions) (*LocalTrackPublication, error) {
	if opts == nil {
		opts = &TrackPublicationOptions{}
	}
	kind := KindFromRTPType(track.Kind())
	// default sources, since clients generally look for camera/mic
	if opts.Source == livekit.TrackSource_UNKNOWN {
		if kind == TrackKindVideo {
			opts.Source = livekit.TrackSource_CAMERA
		} else if kind == TrackKindAudio {
			opts.Source = livekit.TrackSource_MICROPHONE
		}
	}

	publisher, ok := p.engine.Publisher()
	if !ok {
		return nil, ErrNoPeerConnection
	}

	pubChan := make(chan *livekit.TrackPublishedResponse, 1)
	p.engine.RegisterTrackPublishedListener(track.ID(), pubChan)
	defer p.engine.UnregisterTrackPublishedListener(track.ID())

	pub := NewLocalTrackPublication(kind, track, *opts /*, p.client*/)
	pub.onMuteChanged = p.onTrackMuted

	req := &livekit.AddTrackRequest{
		Cid:        track.ID(),
		Name:       opts.Name,
		Source:     opts.Source,
		Type:       kind.ProtoType(),
		Width:      uint32(opts.VideoWidth),
		Height:     uint32(opts.VideoHeight),
		DisableDtx: opts.DisableDTX,
		Stereo:     opts.Stereo,
		Stream:     opts.Stream,
		Encryption: opts.Encryption,
	}
	if kind == TrackKindVideo {
		// single layer
		req.Layers = []*livekit.VideoLayer{
			{
				Quality: livekit.VideoQuality_HIGH,
				Width:   uint32(opts.VideoWidth),
				Height:  uint32(opts.VideoHeight),
			},
		}
	}
	err := p.client.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_AddTrack{
			AddTrack: req,
		},
	})
	if err != nil {
		return nil, err
	}

	// add transceivers - re-use if possible, AddTrack will try to re-use.
	// NOTE: `AddTrack` technically cannot re-use transceiver if it was ever
	// used to send media, i. e. if it was ever in a `sendrecv` or `sendonly`
	// direction. But, pion does not enforce that based on browser behaviour
	// observed in practice.
	sender, err := publisher.PeerConnection().AddTrack(track)
	if err != nil {
		return nil, err
	}

	// LocalTrack will consume rtcp packets so we don't need to consume again
	_, isSampleTrack := track.(*LocalTrack)
	pub.setSender(sender, !isSampleTrack)

	publisher.Negotiate()

	var pubRes *livekit.TrackPublishedResponse
	select {
	case pubRes = <-pubChan:
		break
	case <-time.After(trackPublishTimeout):
		return nil, ErrTrackPublishTimeout
	}

	pub.updateInfo(pubRes.Track)
	p.addPublication(pub)

	p.Callback.OnLocalTrackPublished(pub, p)
	//p.roomCallback.OnLocalTrackPublished(pub, p)

	p.engine.logger.Debugw("published track", "name", opts.Name, "source", opts.Source.String(), "trackID", pubRes.Track.Sid)
	return pub, nil
}

// PublishSimulcastTrack publishes up to three layers to the server
func (p *RelayParticipant) PublishSimulcastTrack(tracks []*LocalTrack, opts *TrackPublicationOptions) (*LocalTrackPublication, error) {
	if len(tracks) == 0 {
		return nil, nil
	}

	for _, track := range tracks {
		if track.Kind() != webrtc.RTPCodecTypeVideo {
			return nil, ErrUnsupportedSimulcastKind
		}
		if track.videoLayer == nil || track.RID() == "" {
			return nil, ErrInvalidSimulcastTrack
		}
	}

	tracksCopy := make([]*LocalTrack, len(tracks))
	copy(tracksCopy, tracks)

	// tracks should be low to high
	sort.Slice(tracksCopy, func(i, j int) bool {
		return tracksCopy[i].videoLayer.Width < tracksCopy[j].videoLayer.Width
	})

	if opts == nil {
		opts = &TrackPublicationOptions{}
	}
	// default sources, since clients generally look for camera/mic
	if opts.Source == livekit.TrackSource_UNKNOWN {
		opts.Source = livekit.TrackSource_CAMERA
	}

	mainTrack := tracksCopy[len(tracksCopy)-1]

	pubChan := make(chan *livekit.TrackPublishedResponse, 1)
	p.engine.RegisterTrackPublishedListener(mainTrack.ID(), pubChan)
	defer p.engine.UnregisterTrackPublishedListener(mainTrack.ID())

	pub := NewLocalTrackPublication(KindFromRTPType(mainTrack.Kind()), nil, *opts /*, p.engine.client*/)
	pub.onMuteChanged = p.onTrackMuted

	var layers []*livekit.VideoLayer
	for _, st := range tracksCopy {
		layers = append(layers, st.videoLayer)
	}
	p.logger.Debugw("send request to add track", "track", mainTrack.ID(), "source", opts.Source.String())

	err := p.engine.client.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_AddTrack{
			AddTrack: &livekit.AddTrackRequest{
				Cid:    mainTrack.ID(),
				Name:   opts.Name,
				Source: opts.Source,
				Type:   pub.Kind().ProtoType(),
				Width:  mainTrack.videoLayer.Width,
				Height: mainTrack.videoLayer.Height,
				Layers: layers,
				SimulcastCodecs: []*livekit.SimulcastCodec{
					{
						Codec: mainTrack.Codec().MimeType,
						Cid:   mainTrack.ID(),
					},
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var pubRes *livekit.TrackPublishedResponse
	select {
	case pubRes = <-pubChan:
		break
	case <-time.After(trackPublishTimeout):
		return nil, ErrTrackPublishTimeout
	}

	publisher, ok := p.engine.Publisher()
	if !ok {
		return nil, ErrNoPeerConnection
	}

	// add transceivers
	publishPC := publisher.PeerConnection()
	var transceiver *webrtc.RTPTransceiver
	var sender *webrtc.RTPSender
	for idx, st := range tracksCopy {
		if idx == 0 {
			// add transceivers - re-use if possible, AddTrack will try to re-use.
			// NOTE: `AddTrack` technically cannot re-use transceiver if it was ever
			// used to send media, i. e. if it was ever in a `sendrecv` or `sendonly`
			// direction. But, pion does not enforce that based on browser behaviour
			// observed in practice.
			sender, err = publishPC.AddTrack(st)
			if err != nil {
				return nil, err
			}

			// as there is no way to get transceiver from sender, search
			for _, tr := range publishPC.GetTransceivers() {
				if tr.Sender() == sender {
					transceiver = tr
					break
				}
			}

			pub.setSender(sender, false)
		} else {
			if err = sender.AddEncoding(st); err != nil {
				return nil, err
			}
		}
		pub.addSimulcastTrack(st)
		st.SetTransceiver(transceiver)
	}

	pub.updateInfo(pubRes.Track)
	p.addPublication(pub)

	publisher.Negotiate()

	p.Callback.OnLocalTrackPublished(pub, p)
	//p.roomCallback.OnLocalTrackPublished(pub, p)

	p.engine.logger.Debugw("published simulcast track", "name", opts.Name, "source", opts.Source.String(), "trackID", pubRes.Track.Sid)

	return pub, nil
}

func (p *RelayParticipant) republishTracks() {
	var localPubs []*LocalTrackPublication
	p.tracks.Range(func(key, value interface{}) bool {
		track := value.(*LocalTrackPublication)

		if track.Track() != nil || len(track.simulcastTracks) > 0 {
			localPubs = append(localPubs, track)
		}
		p.tracks.Delete(key)
		p.audioTracks.Delete(key)
		p.videoTracks.Delete(key)

		p.Callback.OnLocalTrackUnpublished(track, p)
		//p.roomCallback.OnLocalTrackUnpublished(track, p)
		return true
	})

	for _, pub := range localPubs {
		opt := pub.PublicationOptions()
		if len(pub.simulcastTracks) > 0 {
			var tracks []*LocalTrack
			for _, st := range pub.simulcastTracks {
				tracks = append(tracks, st)
			}
			p.PublishSimulcastTrack(tracks, &opt)
		} else if track := pub.TrackLocal(); track != nil {
			_, err := p.PublishTrack(track, &opt)
			if err != nil {
				p.engine.logger.Warnw("could not republish track", err, "track", pub.SID())
			}
		} else {
			p.engine.logger.Warnw("could not republish track as no track local found", nil, "track", pub.SID())
		}
	}
}

func (p *RelayParticipant) closeTracks() {
	var localPubs []*LocalTrackPublication
	p.tracks.Range(func(key, value interface{}) bool {
		track := value.(*LocalTrackPublication)
		if track.Track() != nil || len(track.simulcastTracks) > 0 {
			localPubs = append(localPubs, track)
		}
		p.tracks.Delete(key)
		p.audioTracks.Delete(key)
		p.videoTracks.Delete(key)
		return true
	})

	for _, pub := range localPubs {
		pub.CloseTrack()
	}
}

// PublishData sends custom user data via WebRTC data channel.
//
// By default, the message can be received by all participants in a room,
// see WithDataPublishDestination for choosing specific participants.
//
// Messages are sent via a LOSSY channel by default, see WithDataPublishReliable for sending reliable data.
//
// Deprecated: Use PublishDataPacket with UserData instead.
func (p *RelayParticipant) PublishData(payload []byte, opts ...DataPublishOption) error {
	options := &dataPublishOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return p.PublishDataPacket(UserData(payload), opts...)
}

// PublishDataPacket sends a packet via a WebRTC data channel. UserData can be used for sending custom user data.
//
// By default, the message can be received by all participants in a room,
// see WithDataPublishDestination for choosing specific participants.
//
// Messages are sent via UDP and offer no delivery guarantees, see WithDataPublishReliable for sending data reliably (with retries).
func (p *RelayParticipant) PublishDataPacket(pck DataPacket, opts ...DataPublishOption) error {
	options := &dataPublishOptions{}
	for _, opt := range opts {
		opt(options)
	}
	dataPacket := pck.ToProto()
	if options.Topic != "" {
		if u, ok := dataPacket.Value.(*livekit.DataPacket_User); ok && u.User != nil {
			u.User.Topic = proto.String(options.Topic)
		}
	}

	// This matches the default value of Kind on protobuf level.
	kind := livekit.DataPacket_LOSSY
	if options.Reliable != nil && *options.Reliable {
		kind = livekit.DataPacket_RELIABLE
	}

	dataPacket.DestinationIdentities = options.DestinationIdentities
	if u, ok := dataPacket.Value.(*livekit.DataPacket_User); ok && u.User != nil {
		//lint:ignore SA1019 backward compatibility
		u.User.DestinationIdentities = options.DestinationIdentities
	}

	return p.engine.publishDataPacket(dataPacket, kind)
}

func (p *RelayParticipant) UnpublishTrack(sid string) error {
	obj, loaded := p.tracks.LoadAndDelete(sid)
	if !loaded {
		return ErrCannotFindTrack
	}
	p.audioTracks.Delete(sid)
	p.videoTracks.Delete(sid)

	pub, ok := obj.(*LocalTrackPublication)
	if !ok {
		return nil
	}

	var err error
	if localTrack, ok := pub.track.(webrtc.TrackLocal); ok {
		publisher, ok := p.engine.Publisher()
		if !ok {
			return ErrNoPeerConnection
		}
		for _, sender := range publisher.pc.GetSenders() {
			if sender.Track() == localTrack {
				err = publisher.pc.RemoveTrack(sender)
				break
			}
		}
		p.logger.Debugw("UnpublishTrack: negotiate for publisher")
		publisher.Negotiate()
	}

	pub.CloseTrack()

	p.Callback.OnLocalTrackUnpublished(pub, p)
	//p.roomCallback.OnLocalTrackUnpublished(pub, p)

	p.engine.logger.Debugw("track unpublished", "name", pub.Name(), "trackID", sid)
	return err
}

// GetPublisherPeerConnection is a power-user API that gives access to the underlying publisher peer connection
// local tracks are published to server via this PeerConnection
func (p *RelayParticipant) GetPublisherPeerConnection() *webrtc.PeerConnection {
	if publisher, ok := p.engine.Publisher(); ok {
		return publisher.PeerConnection()
	}
	return nil
}

// SetName sets the name of the current participant.
// updates will be performed only if the participant has canUpdateOwnMetadata grant
func (p *RelayParticipant) SetName(name string) {
	_ = p.engine.client.SendUpdateParticipantMetadata(&livekit.UpdateParticipantMetadata{
		Name: name,
	})
}

// SetMetadata sets the metadata of the current participant.
// Updates will be performed only if the participant has canUpdateOwnMetadata grant.
func (p *RelayParticipant) SetMetadata(metadata string) {
	_ = p.engine.client.SendUpdateParticipantMetadata(&livekit.UpdateParticipantMetadata{
		Metadata: metadata,
	})
}

// SetAttributes sets the KV attributes of the current participant.
// To remove an attribute, set it to empty value.
// Updates will be performed only if the participant has canUpdateOwnMetadata grant.
func (p *RelayParticipant) SetAttributes(attrs map[string]string) {
	_ = p.engine.client.SendUpdateParticipantMetadata(&livekit.UpdateParticipantMetadata{
		Attributes: attrs,
	})
}

func (p *RelayParticipant) updateInfo(info *livekit.ParticipantInfo) {
	p.baseParticipant.updateInfo(info, p)

	// detect tracks that have been muted remotely, and apply changes
	for _, ti := range info.Tracks {
		pub := p.getLocalPublication(ti.Sid)
		if pub == nil {
			continue
		}
		if pub.IsMuted() != ti.Muted {
			_ = p.engine.client.SendMuteTrack(pub.SID(), pub.IsMuted())
		}
	}
}

func (p *RelayParticipant) getLocalPublication(sid string) *LocalTrackPublication {
	if pub, ok := p.getPublication(sid).(*LocalTrackPublication); ok {
		return pub
	}
	return nil
}

func (p *RelayParticipant) onTrackMuted(pub *LocalTrackPublication, muted bool) {
	if muted {
		p.Callback.OnTrackMuted(pub, p)
		//p.roomCallback.OnTrackMuted(pub, p)
	} else {
		p.Callback.OnTrackUnmuted(pub, p)
		//p.roomCallback.OnTrackUnmuted(pub, p)
	}
}

// Control who can subscribe to LocalParticipant's published tracks.
//
// By default, all participants can subscribe. This allows fine-grained control over
// who is able to subscribe at a participant and track level.
//
// Note: if access is given at a track-level (i.e. both `AllParticipants` and
// `TrackPermission.AllTracks` are false), any newer published tracks
// will not grant permissions to any participants and will require a subsequent
// permissions update to allow subscription.
func (p *RelayParticipant) SetSubscriptionPermission(sp *livekit.SubscriptionPermission) {
	p.lock.Lock()
	p.subscriptionPermission = proto.Clone(sp).(*livekit.SubscriptionPermission)
	p.updateSubscriptionPermissionLocked()
	p.lock.Unlock()
}

func (p *RelayParticipant) updateSubscriptionPermission() {
	p.lock.RLock()
	defer p.lock.RUnlock()

	p.updateSubscriptionPermissionLocked()
}

func (p *RelayParticipant) updateSubscriptionPermissionLocked() {
	if p.subscriptionPermission == nil {
		return
	}

	err := p.engine.client.SendRequest(&livekit.SignalRequest{
		Message: &livekit.SignalRequest_SubscriptionPermission{
			SubscriptionPermission: p.subscriptionPermission,
		},
	})
	if err != nil {
		p.logger.Errorw(
			"could not send subscription permission", err,
			"participant", p.identity,
			"pID", p.sid,
		)
	}
}

// StreamText creates a new text stream writer with the provided options.
func (p *RelayParticipant) StreamText(options StreamTextOptions) *TextStreamWriter {
	if options.StreamId == nil {
		streamId := uuid.New().String()
		options.StreamId = &streamId
	}

	if options.Attributes == nil {
		options.Attributes = make(map[string]string)
	}

	var totalSize *uint64
	if options.TotalSize != 0 {
		totalSize = &options.TotalSize
	}

	info := TextStreamInfo{
		baseStreamInfo: &baseStreamInfo{
			Id:         *options.StreamId,
			MimeType:   "text/plain",
			Topic:      options.Topic,
			Timestamp:  time.Now().UnixMilli(),
			Size:       totalSize,
			Attributes: options.Attributes,
		},
	}

	header := &livekit.DataStream_Header{
		StreamId:    info.Id,
		MimeType:    info.MimeType,
		Topic:       info.Topic,
		Timestamp:   info.Timestamp,
		TotalLength: info.Size,
		Attributes:  info.Attributes,
		ContentHeader: &livekit.DataStream_Header_TextHeader{
			TextHeader: &livekit.DataStream_TextHeader{
				OperationType:     livekit.DataStream_CREATE,
				AttachedStreamIds: options.AttachedStreamIds,
			},
		},
	}
	if options.ReplyToStreamId != nil {
		if textHeader, ok := header.ContentHeader.(*livekit.DataStream_Header_TextHeader); ok {
			textHeader.TextHeader.ReplyToStreamId = *options.ReplyToStreamId
		}
	}

	writer := newTextStreamWriter(info, header, p.engine, options.DestinationIdentities, options.OnProgress)

	p.engine.AddOnClose(func() {
		writer.Close()
	})

	return writer
}

// SendText creates a new text stream writer with the provided options.
// It will return a TextStreamInfo that can be used to get metadata about the stream.
func (p *RelayParticipant) SendText(text string, options StreamTextOptions) *TextStreamInfo {
	if options.TotalSize == 0 {
		textInBytes := []byte(text)
		options.TotalSize = uint64(len(textInBytes))
	}

	// Ensure that the number of attached stream ids matches the number of attachments, generate if necessary
	attachedStreamIds := options.AttachedStreamIds
	numberOfAttachments := len(options.Attachments)
	numberOfAttachedStreamIds := len(attachedStreamIds)
	if numberOfAttachments > 0 {
		if numberOfAttachedStreamIds != numberOfAttachments {
			for i := numberOfAttachedStreamIds; i < numberOfAttachments; i++ {
				attachedStreamIds = append(attachedStreamIds, uuid.New().String())
			}
		}
	}
	options.AttachedStreamIds = attachedStreamIds

	var progresses sync.Map
	for i := range numberOfAttachments + 1 {
		progresses.Store(i, float64(0))
	}

	handleProgress := func(progress float64, id int) {
		progresses.Store(id, progress)

		var totalProgress float64
		progresses.Range(func(_, value interface{}) bool {
			totalProgress += value.(float64)
			return true
		})

		if options.OnProgress != nil {
			options.OnProgress(totalProgress / float64(numberOfAttachments+1))
		}
	}

	textOptions := options
	textOnProgress := func(progress float64) {
		handleProgress(progress, 0)
	}
	textOptions.OnProgress = textOnProgress
	writer := p.StreamText(textOptions)

	onDone := func() {
		writer.Close()
	}
	writer.Write(text, &onDone)

	for i, attachment := range options.Attachments {
		onProgress := func(progress float64) {
			handleProgress(progress, i+1)
		}
		p.SendFile(attachment, StreamBytesOptions{
			Topic:                 options.Topic,
			DestinationIdentities: options.DestinationIdentities,
			StreamId:              &attachedStreamIds[i],
			OnProgress:            onProgress,
			Attributes:            options.Attributes,
		})
	}

	return &writer.Info
}

// StreamBytes creates a new byte stream writer with the provided options.
func (p *RelayParticipant) StreamBytes(options StreamBytesOptions) *ByteStreamWriter {
	if options.StreamId == nil {
		streamId := uuid.New().String()
		options.StreamId = &streamId
	}

	if options.Attributes == nil {
		options.Attributes = make(map[string]string)
	}

	var totalSize *uint64
	if options.TotalSize != 0 {
		totalSize = &options.TotalSize
	}

	info := ByteStreamInfo{
		baseStreamInfo: &baseStreamInfo{
			Id:         *options.StreamId,
			MimeType:   options.MimeType,
			Topic:      options.Topic,
			Timestamp:  time.Now().UnixMilli(),
			Size:       totalSize,
			Attributes: options.Attributes,
		},
	}

	header := &livekit.DataStream_Header{
		StreamId:    info.Id,
		MimeType:    info.MimeType,
		Topic:       info.Topic,
		Timestamp:   info.Timestamp,
		TotalLength: info.Size,
		Attributes:  info.Attributes,
		ContentHeader: &livekit.DataStream_Header_ByteHeader{
			ByteHeader: &livekit.DataStream_ByteHeader{},
		},
	}

	if options.FileName != nil {
		if byteHeader, ok := header.ContentHeader.(*livekit.DataStream_Header_ByteHeader); ok {
			byteHeader.ByteHeader.Name = *options.FileName
		}
		info.Name = options.FileName
	}

	writer := newByteStreamWriter(info, header, p.engine, options.DestinationIdentities, options.OnProgress)

	p.engine.AddOnClose(func() {
		writer.Close()
	})

	return writer
}

// SendFile sends a file to the remote participant as a byte stream with the provided options.
// It will return a ByteStreamInfo that can be used to get metadata about the stream.
// Error is returned if the file cannot be read.
func (p *RelayParticipant) SendFile(filePath string, options StreamBytesOptions) (*ByteStreamInfo, error) {
	if options.TotalSize == 0 {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return nil, err
		}
		options.TotalSize = uint64(fileInfo.Size())
	}

	if options.MimeType == "" {
		mimeType := mime.TypeByExtension(filepath.Ext(filePath))
		options.MimeType = mimeType
	}

	writer := p.StreamBytes(options)

	fileBytes, err := os.ReadFile(filePath)
	if err != nil {
		writer.Close()
		return nil, err
	}

	onDone := func() {
		writer.Close()
	}
	writer.Write(fileBytes, &onDone)

	return &writer.Info, nil
}
