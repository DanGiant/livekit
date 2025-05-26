package rtc

import (
	"github.com/livekit/livekit-server/pkg/rtc/relay"
	"sync"
	"time"

	sutils "github.com/livekit/livekit-server/pkg/utils"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"

	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
)

const (
	relayDebounceInterval = 100 * time.Millisecond
)

type RelayedTrackParams struct {
	PublisherID       livekit.ParticipantID
	PublisherIdentity livekit.ParticipantIdentity
	PublisherVersion  uint32
	DestNodeID        livekit.NodeID
	RelayParticipant  *relay.RelayParticipant
	MediaTrack        types.MediaTrack
	DownTrack         *sfu.RelayDownTrack
	LocalTracks       []*relay.LocalTrack
	LocalPublication  *relay.LocalTrackPublication
	IsSimulcast       bool
	RelayedQualities  []types.SubscribedCodecQuality
	//AdaptiveStream    bool
}

type RelayedTrack struct {
	params RelayedTrackParams
	logger logger.Logger
	sender atomic.Pointer[webrtc.RTPSender]
	//needsNegotiation atomic.Bool

	versionGenerator utils.TimedVersionGenerator
	settingsLock     sync.Mutex
	settings         *livekit.UpdateTrackSettings
	settingsVersion  utils.TimedVersion

	//bindLock        sync.Mutex
	//bound           bool
	//onBindCallbacks []func(error)

	onClose atomic.Value // func(bool)

	//debouncer func(func())
}

func NewRelayedTrack(params RelayedTrackParams) *RelayedTrack {
	s := &RelayedTrack{
		params: params,
		logger: params.RelayParticipant.GetLogger().WithComponent(sutils.ComponentRelay).WithValues(
			"trackID", params.DownTrack.ID(),
			"destNodeID", params.DownTrack.RelayDestNodeID(),
			"publisherID", params.PublisherID,
			"publisher", params.PublisherIdentity,
		),
		versionGenerator: utils.NewDefaultTimedVersionGenerator(),
		//debouncer:        debounce.New(relayDebounceInterval),
	}

	return s
}

//func (t *RelayedTrack) AddOnBind(f func(error)) {
//	t.bindLock.Lock()
//	bound := t.bound
//	if !bound {
//		t.onBindCallbacks = append(t.onBindCallbacks, f)
//	}
//	t.bindLock.Unlock()
//
//	if bound {
//		// fire immediately, do not need to persist since bind is a one time event
//		go f(nil)
//	}
//}

// for DownTrack callback to notify us that it's bound
//func (t *RelayedTrack) Bound(err error) {
//	t.bindLock.Lock()
//	if err == nil {
//		t.bound = true
//	}
//	callbacks := t.onBindCallbacks
//	t.onBindCallbacks = nil
//	t.bindLock.Unlock()
//
//	if err == nil && t.MediaTrack().Kind() == livekit.TrackType_VIDEO {
//		// When AdaptiveStream is enabled, default the subscriber to LOW quality stream
//		// we would want LOW instead of OFF for a couple of reasons
//		// 1. when a subscriber unsubscribes from a track, we would forget their previously defined settings
//		//    depending on client implementation, subscription on/off is kept separately from adaptive stream
//		//    So when there are no changes to desired resolution, but the user re-subscribes, we may leave stream at OFF
//		// 2. when interacting with dynacast *and* adaptive stream. If the publisher was not publishing at the
//		//    time of subscription, we might not be able to trigger adaptive stream updates on the client side
//		//    (since there isn't any video frames coming through). this will leave the stream "stuck" on off, without
//		//    a trigger to re-enable it
//		t.settingsLock.Lock()
//		if t.settings != nil {
//			if t.params.AdaptiveStream {
//				// remove `disabled` flag to force a visibility update
//				t.settings.Disabled = false
//			}
//		} else {
//			if t.params.AdaptiveStream {
//				t.settings = &livekit.UpdateTrackSettings{Quality: livekit.VideoQuality_LOW}
//			} else {
//				t.settings = &livekit.UpdateTrackSettings{Quality: livekit.VideoQuality_HIGH}
//			}
//		}
//		t.settingsLock.Unlock()
//		t.applySettings()
//	}
//
//	for _, cb := range callbacks {
//		go cb(err)
//	}
//}

// for DownTrack callback to notify us that it's closed
func (t *RelayedTrack) Close(isExpectedToResume bool) {
	if onClose := t.onClose.Load(); onClose != nil {
		go onClose.(func(bool))(isExpectedToResume)
	}
}

func (t *RelayedTrack) OnClose(f func(bool)) {
	t.onClose.Store(f)
}

//func (t *RelayedTrack) IsBound() bool {
//	t.bindLock.Lock()
//	defer t.bindLock.Unlock()
//
//	return t.bound
//}

func (t *RelayedTrack) ID() livekit.TrackID {
	return livekit.TrackID(t.params.DownTrack.ID())
}

func (t *RelayedTrack) PublisherID() livekit.ParticipantID {
	return t.params.PublisherID
}

func (t *RelayedTrack) PublisherIdentity() livekit.ParticipantIdentity {
	return t.params.PublisherIdentity
}

func (t *RelayedTrack) PublisherVersion() uint32 {
	return t.params.PublisherVersion
}

func (t *RelayedTrack) DestNodeID() livekit.NodeID {
	return t.params.DestNodeID
}

func (t *RelayedTrack) RelayParticipant() *relay.RelayParticipant {
	return t.params.RelayParticipant
}

func (t *RelayedTrack) DownTrack() *sfu.RelayDownTrack {
	return t.params.DownTrack
}

func (t *RelayedTrack) MediaTrack() types.MediaTrack {
	return t.params.MediaTrack
}

// has subscriber indicated it wants to mute this track
//func (t *RelayedTrack) IsMuted() bool {
//	t.settingsLock.Lock()
//	defer t.settingsLock.Unlock()
//
//	return t.isMutedLocked()
//}

//func (t *RelayedTrack) isMutedLocked() bool {
//	if t.settings == nil {
//		return false
//	}
//
//	return t.settings.Disabled
//}

//func (t *RelayedTrack) SetPublisherMuted(muted bool) {
//	t.DownTrack().PubMute(muted)
//}

//func (t *RelayedTrack) UpdateSubscriberSettings(settings *livekit.UpdateTrackSettings, isImmediate bool) {
//	t.settingsLock.Lock()
//	if proto.Equal(t.settings, settings) {
//		t.settingsLock.Unlock()
//		return
//	}
//
//	isImmediate = isImmediate || (!settings.Disabled && settings.Disabled != t.isMutedLocked())
//	t.settings = utils.CloneProto(settings)
//	t.settingsLock.Unlock()
//
//	if isImmediate {
//		t.applySettings()
//	} else {
//		// avoid frequent changes to mute & video layers, unless it became visible
//		t.debouncer(t.applySettings)
//	}
//}

//func (t *RelayedTrack) UpdateVideoLayer() {
//	t.applySettings()
//}

//func (t *RelayedTrack) applySettings() {
//	t.settingsLock.Lock()
//	if t.settings == nil {
//		t.settingsLock.Unlock()
//		return
//	}
//
//	t.logger.Debugw("updating subscriber track settings", "settings", logger.Proto(t.settings))
//	t.settingsVersion = t.versionGenerator.Next()
//	settingsVersion := t.settingsVersion
//	t.settingsLock.Unlock()
//
//	dt := t.DownTrack()
//	spatial := buffer.InvalidLayerSpatial
//	temporal := buffer.InvalidLayerTemporal
//	if dt.Kind() == webrtc.RTPCodecTypeVideo {
//		mt := t.MediaTrack()
//		quality := t.settings.Quality
//		if t.settings.Width > 0 {
//			quality = mt.GetQualityForDimension(t.settings.Width, t.settings.Height)
//		}
//
//		spatial = buffer.VideoQualityToSpatialLayer(quality, mt.ToProto())
//		if t.settings.Fps > 0 {
//			temporal = mt.GetTemporalLayerForSpatialFps(spatial, t.settings.Fps, dt.Mime())
//		}
//	}
//
//	t.settingsLock.Lock()
//	if settingsVersion != t.settingsVersion {
//		// a newer settings has superceded this one
//		t.settingsLock.Unlock()
//		return
//	}
//
//	if t.settings.Disabled {
//		dt.Mute(true)
//		t.settingsLock.Unlock()
//		return
//	} else {
//		dt.Mute(false)
//	}
//
//	if dt.Kind() == webrtc.RTPCodecTypeVideo {
//		dt.SetMaxSpatialLayer(spatial)
//		if temporal != buffer.InvalidLayerTemporal {
//			dt.SetMaxTemporalLayer(temporal)
//		}
//	}
//	t.settingsLock.Unlock()
//}

//func (t *RelayedTrack) NeedsNegotiation() bool {
//	return t.needsNegotiation.Load()
//}

//func (t *RelayedTrack) SetNeedsNegotiation(needs bool) {
//	t.needsNegotiation.Store(needs)
//}

func (t *RelayedTrack) RTPSender() *webrtc.RTPSender {
	return t.sender.Load()
}

func (t *RelayedTrack) GetLocalPublication() *relay.LocalTrackPublication {
	return t.params.LocalPublication
}

func (t *RelayedTrack) SetRTPSender(sender *webrtc.RTPSender) {
	t.sender.Store(sender)
}

func (t *RelayedTrack) IsSimulcast() bool {
	return t.params.IsSimulcast
}

func (t *RelayedTrack) GetRelayedQualities() []types.SubscribedCodecQuality {
	return t.params.RelayedQualities
}
