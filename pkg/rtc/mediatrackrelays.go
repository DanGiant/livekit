package rtc

import (
	"errors"
	"github.com/livekit/livekit-server/pkg/rtc/relay"
	"sync"

	"github.com/livekit/livekit-server/pkg/sfu/mime"
	sutils "github.com/livekit/livekit-server/pkg/utils"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/webrtc/v4"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/telemetry"
)

var (
	errAlreadyRelayed = errors.New("already relayed")
)

// MediaTrackRelays manages subscriptions of a media track
type MediaTrackRelays struct {
	params MediaTrackRelaysParams

	relayedTracksMu sync.RWMutex
	relayedTracks   map[livekit.NodeID]types.RelayedTrack

	onRelayDownTrackCreated      func(downTrack *sfu.RelayDownTrack)
	onSubscriberMaxQualityChange func(subscriberID livekit.ParticipantID, mime mime.MimeType, layer int32)
}

type MediaTrackRelaysParams struct {
	MediaTrack types.MediaTrack
	IsRelayed  bool

	ReceiverConfig   ReceiverConfig
	SubscriberConfig DirectionConfig

	Telemetry telemetry.TelemetryService

	Logger logger.Logger
}

func NewMediaTrackRelays(params MediaTrackRelaysParams) *MediaTrackRelays {
	return &MediaTrackRelays{
		params:        params,
		relayedTracks: make(map[livekit.NodeID]types.RelayedTrack),
	}
}

func (t *MediaTrackRelays) OnRelayDownTrackCreated(f func(downTrack *sfu.RelayDownTrack)) {
	t.onRelayDownTrackCreated = f
}

//func (t *MediaTrackRelays) OnSubscriberMaxQualityChange(f func(subscriberID livekit.ParticipantID, mime mime.MimeType, layer int32)) {
//	t.onSubscriberMaxQualityChange = f
//}

//func (t *MediaTrackRelays) SetMuted(muted bool) {
//	// update mute of all subscribed tracks
//	for _, st := range t.getAllRelayedTracks() {
//		st.SetPublisherMuted(muted)
//	}
//}

func (t *MediaTrackRelays) IsRelayDestNode(destNodeID livekit.NodeID) bool {
	t.relayedTracksMu.RLock()
	defer t.relayedTracksMu.RUnlock()

	_, ok := t.relayedTracks[destNodeID]
	return ok
}

// AddRelay add relay to remote node to current mediaTrack
func (t *MediaTrackRelays) AddRelay(rp *relay.RelayParticipant, wr *WrappedReceiver) (types.RelayedTrack, error) {
	trackID := t.params.MediaTrack.ID()
	destNodeID := rp.DestNodeID()

	// don't relay the same track to same node multiple times
	t.relayedTracksMu.Lock()
	if _, ok := t.relayedTracks[destNodeID]; ok {
		t.relayedTracksMu.Unlock()
		return nil, errAlreadyRelayed
	}
	t.relayedTracksMu.Unlock()

	var rtcpFeedback []webrtc.RTCPFeedback
	//var maxTrack int
	switch t.params.MediaTrack.Kind() {
	case livekit.TrackType_AUDIO:
		rtcpFeedback = t.params.SubscriberConfig.RTCPFeedback.Audio
		//maxTrack = t.params.ReceiverConfig.PacketBufferSizeAudio
	case livekit.TrackType_VIDEO:
		rtcpFeedback = t.params.SubscriberConfig.RTCPFeedback.Video
		//maxTrack = t.params.ReceiverConfig.PacketBufferSizeVideo
	}
	codecs := wr.Codecs()
	for _, c := range codecs {
		c.RTCPFeedback = rtcpFeedback
	}

	streamID := wr.StreamID()
	//if sub.SupportsSyncStreamID() && t.params.MediaTrack.Stream() != "" {
	//	streamID = PackSyncStreamID(t.params.MediaTrack.PublisherID(), t.params.MediaTrack.Stream())
	//}

	var trailer []byte
	//if t.params.MediaTrack.IsEncrypted() {
	//	trailer = sub.GetTrailer()
	//}

	track := t.params.MediaTrack
	receivers := track.Receivers()
	localTracks := make([]*relay.LocalTrack, 0)
	localTrackSenders := make([]sfu.LocalTrackSender, 0)
	relayedQualities := make([]types.SubscribedCodecQuality, 0)
	var trackPub *relay.LocalTrackPublication

	if t.params.MediaTrack.IsSimulcast() {
		t.params.Logger.Debugw("add relay: track receivers", "ReceiversNum", len(receivers))

		if len(receivers) > 0 {
			for index, receiver := range receivers {
				trackInfo := receiver.TrackInfo()
				t.params.Logger.Debugw("add relay: simulcast track's info", "index", index, "trackInfo", trackInfo)
			}

			receiver := receivers[0]
			trackInfo := receiver.TrackInfo()
			layers := trackInfo.GetLayers()

			t.params.Logger.Debugw("add relay: original simulcast track's info",
				"trackInfo", trackInfo, "layers", layers)

			if layers != nil && len(layers) > 0 {

				for _, layer := range layers {
					t.params.Logger.Debugw("add relay: create simulcast local track",
						"Quality", layer.Quality.String())

					localTrack, err := relay.NewLocalTrack(t.params.Logger, receiver.Codec().RTPCodecCapability,
						relay.WithSimulcast(string(receiver.TrackID()), layer))
					if err != nil {
						t.params.Logger.Errorw("relay manager: create local track for simulcast failed", err,
							"Track", t.params.MediaTrack.ID())
						return nil, err
					}
					localTracks = append(localTracks, localTrack)
					localTrackSenders = append(localTrackSenders, localTrack)
					relayedQualities = append(relayedQualities, types.SubscribedCodecQuality{
						CodecMime: mime.NormalizeMimeType(trackInfo.MimeType),
						Quality:   layer.Quality,
					})
				}
			}

			t.params.Logger.Debugw("add relay: local tracks", "LocalTracksNum", len(localTracks))

			if len(localTracks) > 0 {
				var err error
				trackPub, err = rp.PublishSimulcastTrack(localTracks, &relay.TrackPublicationOptions{
					Name:        track.Name(),
					Source:      track.Source(),
					Stream:      track.Stream(),
					VideoWidth:  int(trackInfo.Width),
					VideoHeight: int(trackInfo.Height),
					Stereo:      trackInfo.Stereo,
					DisableDTX:  trackInfo.DisableDtx,
					Encryption:  trackInfo.Encryption,
				})
				if err != nil {
					t.params.Logger.Errorw("add relay: publish simulcast track failed", err, "Track", track.ID())
					return nil, err
				}
				t.params.Logger.Infow("add relay for track: publish simulcast track success", "Track", track.ID())
			}
		}
	} else {
		t.params.Logger.Debugw("add relay: track is NOT simulcast",
			"destNodeID", rp.DestNodeID(), "TrackID", track.ID(), "ReceiversNum", len(receivers))

		if len(receivers) > 0 {
			for index, receiver := range receivers {
				trackInfo := receiver.TrackInfo()
				t.params.Logger.Debugw("add relay: track's info", "index", index, "trackInfo", trackInfo)
			}
			receiver := receivers[0]
			trackInfo := receiver.TrackInfo()

			t.params.Logger.Debugw("add relay: original track's info", "trackInfo", trackInfo)

			rtpCodec := receiver.Codec().RTPCodecCapability
			if mime.NormalizeMimeType(rtpCodec.MimeType) == mime.MimeTypeRED {
				rtpCodec.MimeType = mime.MimeTypeOpus.String()
			}
			localTrack, err := relay.NewLocalTrack(t.params.Logger, rtpCodec)
			if err != nil {
				t.params.Logger.Errorw("add relay: create local track failed", err, "Track", track.ID())
			}
			localTracks = append(localTracks, localTrack)
			localTrackSenders = append(localTrackSenders, localTrack)

			trackPub, err = rp.PublishTrack(localTrack, &relay.TrackPublicationOptions{
				Name:        track.Name(),
				Source:      track.Source(),
				Stream:      track.Stream(),
				VideoWidth:  int(trackInfo.Width),
				VideoHeight: int(trackInfo.Height),
				Stereo:      trackInfo.Stereo,
				DisableDTX:  trackInfo.DisableDtx,
				Encryption:  trackInfo.Encryption,
			})
			if err != nil {
				t.params.Logger.Errorw("add relay: publish relay track failed", err,
					"destNodeID", rp.DestNodeID(), "Track", track.ID())
				return nil, err
			}
			t.params.Logger.Infow("add relay for track: publish track success",
				"destNodeID", rp.DestNodeID(), "Track", track.ID())
		}
	}

	relayDownTrack, err := sfu.NewRelayDownTrack(sfu.RelayDownTrackParams{
		Codecs:            codecs,
		Source:            t.params.MediaTrack.Source(),
		Receiver:          wr,
		RelayDestNodeID:   rp.DestNodeID(),
		StreamID:          streamID,
		PlayoutDelayLimit: &livekit.PlayoutDelay{Enabled: false},
		Trailer:           trailer,
		Logger:            LoggerWithTrack(rp.GetLogger().WithComponent(sutils.ComponentRelay), trackID, false),
		IsSimulcast:       track.IsSimulcast(),
		LocalTrackSenders: localTrackSenders,
		//RTCPWriter:                     rp.WriteSubscriberRTCP,
		//DisableSenderReportPassThrough: rp.GetDisableSenderReportPassThrough(),
		//SupportsCodecChange:            rp.SupportsCodecChange(),
	})
	if err != nil {
		return nil, err
	}

	if t.onRelayDownTrackCreated != nil {
		t.onRelayDownTrackCreated(relayDownTrack)
	}

	relayedTrack := NewRelayedTrack(RelayedTrackParams{
		PublisherID:       t.params.MediaTrack.PublisherID(),
		PublisherIdentity: t.params.MediaTrack.PublisherIdentity(),
		PublisherVersion:  t.params.MediaTrack.PublisherVersion(),
		DestNodeID:        rp.DestNodeID(),
		RelayParticipant:  rp,
		MediaTrack:        t.params.MediaTrack,
		DownTrack:         relayDownTrack,
		LocalTracks:       localTracks,
		LocalPublication:  trackPub,
		IsSimulcast:       track.IsSimulcast(),
		RelayedQualities:  relayedQualities,
	})

	// force RED to Opus when relay
	codec := receivers[0].Codec().RTPCodecCapability
	if mime.NormalizeMimeType(codec.MimeType) == mime.MimeTypeRED {
		codec.MimeType = mime.MimeTypeOpus.String()
	}
	if !wr.DetermineReceiver(codec) {
		rp.GetLogger().Errorw(
			"could not add down track", webrtc.ErrNoCodecsAvailable,
			"publisher", relayedTrack.PublisherIdentity(),
			"publisherID", relayedTrack.PublisherID(),
			"trackID", trackID,
		)
	}

	if err = wr.AddRelayDownTrack(relayDownTrack); err != nil && !errors.Is(err, sfu.ErrReceiverClosed) {
		rp.GetLogger().Errorw(
			"could not add down track", err,
			"publisher", relayedTrack.PublisherIdentity(),
			"publisherID", relayedTrack.PublisherID(),
			"trackID", trackID,
		)
	}

	//downTrack.OnStatsUpdate(func(_ *sfu.DownTrack, stat *livekit.AnalyticsStat) {
	//	key := telemetry.StatsKeyForTrack(livekit.StreamType_DOWNSTREAM, subscriberID, trackID, t.params.MediaTrack.Source(), t.params.MediaTrack.Kind())
	//	t.params.Telemetry.TrackStats(key, stat)
	//})
	//
	//downTrack.OnRttUpdate(func(_ *sfu.DownTrack, rtt uint32) {
	//	go sub.UpdateMediaRTT(rtt)
	//})
	//
	//downTrack.AddReceiverReportListener(func(dt *sfu.DownTrack, report *rtcp.ReceiverReport) {
	//	sub.HandleReceiverReport(dt, report)
	//})
	//
	//var transceiver *webrtc.RTPTransceiver
	//var sender *webrtc.RTPSender

	// it is possible that subscribed track is closed before subscription manager sets
	// the `OnClose` callback. That handler in subscription manager removes the track
	// from the peer connection.
	//
	// But, the subscription could be removed early if the published track is closed
	// while adding subscription. In those cases, subscription manager would not have set
	// the `OnClose` callback. So, set it here to handle cases of early close.

	//relayedTrack.OnClose(func(isExpectedToResume bool) {
	//	if !isExpectedToResume {
	//		if err := sub.RemoveTrackLocal(sender); err != nil {
	//			t.params.Logger.Warnw("could not remove track from peer connection", err)
	//		}
	//	}
	//})

	//downTrack.SetTransceiver(transceiver)

	relayDownTrack.OnCloseHandler(func(isExpectedToResume bool) {
		t.relayDownTrackClosed(rp.DestNodeID(), relayedTrack, isExpectedToResume)
	})

	t.relayedTracksMu.Lock()
	t.relayedTracks[rp.DestNodeID()] = relayedTrack
	t.relayedTracksMu.Unlock()

	return relayedTrack, nil
}

// RemoveSubscriber removes participant from subscription
// stop all forwarders to the client
func (t *MediaTrackRelays) RemoveRelay(destNodeID livekit.NodeID, isExpectedToResume bool) error {
	relayedTrack := t.getRelayedTrack(destNodeID)
	if relayedTrack == nil {
		return errNotFound
	}

	t.params.Logger.Debugw("removing relay for node", "destNodeID", destNodeID, "isExpectedToResume", isExpectedToResume)
	t.closeRelayedTrack(relayedTrack, isExpectedToResume)
	return nil
}

func (t *MediaTrackRelays) closeRelayedTrack(relayedTrack types.RelayedTrack, isExpectedToResume bool) {
	dt := relayedTrack.DownTrack()
	if dt == nil {
		return
	}

	if isExpectedToResume {
		dt.CloseWithFlush(false)
	} else {
		// flushing blocks, avoid blocking when publisher removes all its subscribers
		go dt.CloseWithFlush(true)
	}
}

func (t *MediaTrackRelays) GetAllDestNodes() []livekit.NodeID {
	t.relayedTracksMu.RLock()
	defer t.relayedTracksMu.RUnlock()

	relays := make([]livekit.NodeID, 0, len(t.relayedTracks))
	for id := range t.relayedTracks {
		relays = append(relays, id)
	}
	return relays
}

func (t *MediaTrackRelays) GetAllDestNodesForMime(mime mime.MimeType) []livekit.NodeID {
	t.relayedTracksMu.RLock()
	defer t.relayedTracksMu.RUnlock()

	relays := make([]livekit.NodeID, 0, len(t.relayedTracks))
	for id, relayedTrack := range t.relayedTracks {
		if relayedTrack.DownTrack().Mime() != mime {
			continue
		}

		relays = append(relays, id)
	}
	return relays
}

func (t *MediaTrackRelays) GetNumRelays() int {
	t.relayedTracksMu.RLock()
	defer t.relayedTracksMu.RUnlock()

	return len(t.relayedTracks)
}

//func (t *MediaTrackRelays) UpdateVideoLayers() {
//	for _, st := range t.getAllSubscribedTracks() {
//		st.UpdateVideoLayer()
//	}
//}

func (t *MediaTrackRelays) getRelayedTrack(destNodeID livekit.NodeID) types.RelayedTrack {
	t.relayedTracksMu.RLock()
	defer t.relayedTracksMu.RUnlock()

	return t.relayedTracks[destNodeID]
}

func (t *MediaTrackRelays) getAllRelayedTracks() []types.RelayedTrack {
	t.relayedTracksMu.RLock()
	defer t.relayedTracksMu.RUnlock()

	return t.getAllRelayedTracksLocked()
}

func (t *MediaTrackRelays) getAllRelayedTracksLocked() []types.RelayedTrack {
	relayedTracks := make([]types.RelayedTrack, 0, len(t.relayedTracks))
	for _, relayedTrack := range t.relayedTracks {
		relayedTracks = append(relayedTracks, relayedTrack)
	}
	return relayedTracks
}

//func (t *MediaTrackRelays) DebugInfo() []map[string]interface{} {
//	relayedTrackInfo := make([]map[string]interface{}, 0)
//	for _, val := range t.getAllRelayedTracks() {
//		if st, ok := val.(*RelayedTrack); ok {
//			relayedTrackInfo = append(relayedTrackInfo, st.DownTrack().DebugInfo())
//		}
//	}
//
//	return relayedTrackInfo
//}

func (t *MediaTrackRelays) relayDownTrackClosed(
	destNodeID livekit.NodeID,
	relayedTrack types.RelayedTrack,
	isExpectedToResume bool,
) {
	// Cache transceiver for potential re-use on resume.
	// To ensure subscription manager does not re-subscribe before caching,
	// delete the subscribed track only after caching.
	//if isExpectedToResume {
	//	dt := subTrack.DownTrack()
	//	tr := dt.GetTransceiver()
	//	if tr != nil {
	//		sub.CacheDownTrack(subTrack.ID(), tr, dt.GetState())
	//	}
	//}

	go func() {
		t.relayedTracksMu.Lock()
		delete(t.relayedTracks, destNodeID)
		t.relayedTracksMu.Unlock()
		relayedTrack.Close(isExpectedToResume)
	}()
}
