package relay

import (
	"github.com/livekit/protocol/livekit"
)

type DisconnectionReason string

const (
	LeaveRequested     DisconnectionReason = "leave requested by user"
	UserUnavailable    DisconnectionReason = "remote user unavailable"
	RejectedByUser     DisconnectionReason = "rejected by remote user"
	Failed             DisconnectionReason = "connection to room failed"
	RoomClosed         DisconnectionReason = "room closed"
	ParticipantRemoved DisconnectionReason = "removed by server"
	DuplicateIdentity  DisconnectionReason = "duplicate identity"
	OtherReason        DisconnectionReason = "other reasons"
)

func GetDisconnectionReason(reason livekit.DisconnectReason) DisconnectionReason {
	// TODO: SDK should forward the original reason and provide helpers like IsRequestedLeave.
	r := OtherReason
	switch reason {
	case livekit.DisconnectReason_CLIENT_INITIATED:
		r = LeaveRequested
	case livekit.DisconnectReason_USER_UNAVAILABLE:
		r = UserUnavailable
	case livekit.DisconnectReason_USER_REJECTED:
		r = RejectedByUser
	case livekit.DisconnectReason_ROOM_CLOSED:
		r = RoomClosed
	case livekit.DisconnectReason_PARTICIPANT_REMOVED:
		r = ParticipantRemoved
	case livekit.DisconnectReason_DUPLICATE_IDENTITY:
		r = DuplicateIdentity
	case livekit.DisconnectReason_JOIN_FAILURE, livekit.DisconnectReason_SIGNAL_CLOSE, livekit.DisconnectReason_STATE_MISMATCH:
		r = Failed
	}
	return r
}

//------------------------------------------------------------------

// ParticipantAttributesChangedFunc is callback for Participant attribute change event.
// The function is called with an already updated participant state and the map of changes attributes.
// Deleted attributes will have empty string value in the changed map.
type ParticipantAttributesChangedFunc func(changed map[string]string, p ClientParticipant)

type ParticipantCallback struct {
	// for local participant
	OnLocalTrackPublished   func(publication *LocalTrackPublication, lp *RelayParticipant)
	OnLocalTrackUnpublished func(publication *LocalTrackPublication, lp *RelayParticipant)

	// for all participants
	OnTrackMuted               func(pub TrackPublication, p ClientParticipant)
	OnTrackUnmuted             func(pub TrackPublication, p ClientParticipant)
	OnMetadataChanged          func(oldMetadata string, p ClientParticipant)
	OnAttributesChanged        ParticipantAttributesChangedFunc
	OnIsSpeakingChanged        func(p ClientParticipant)
	OnConnectionQualityChanged func(update *livekit.ConnectionQualityInfo, p ClientParticipant)

	// for remote participants
	//OnTrackSubscribed         func(track *webrtc.TrackRemote, publication *RemoteTrackPublication, rp *RemoteParticipant)
	//OnTrackUnsubscribed       func(track *webrtc.TrackRemote, publication *RemoteTrackPublication, rp *RemoteParticipant)
	//OnTrackSubscriptionFailed func(sid string, rp *RemoteParticipant)
	//OnTrackPublished          func(publication *RemoteTrackPublication, rp *RemoteParticipant)
	//OnTrackUnpublished        func(publication *RemoteTrackPublication, rp *RemoteParticipant)
	//OnDataReceived func(data []byte, params DataReceiveParams) // Deprecated: Use OnDataPacket instead
	//OnDataPacket func(data DataPacket, params DataReceiveParams)
	//OnTranscriptionReceived func(transcriptionSegments []*TranscriptionSegment, p Participant, publication TrackPublication)
}

func NewParticipantCallback() *ParticipantCallback {
	return &ParticipantCallback{
		OnLocalTrackPublished:   func(publication *LocalTrackPublication, lp *RelayParticipant) {},
		OnLocalTrackUnpublished: func(publication *LocalTrackPublication, lp *RelayParticipant) {},

		OnTrackMuted:               func(pub TrackPublication, p ClientParticipant) {},
		OnTrackUnmuted:             func(pub TrackPublication, p ClientParticipant) {},
		OnMetadataChanged:          func(oldMetadata string, p ClientParticipant) {},
		OnAttributesChanged:        func(changed map[string]string, p ClientParticipant) {},
		OnIsSpeakingChanged:        func(p ClientParticipant) {},
		OnConnectionQualityChanged: func(update *livekit.ConnectionQualityInfo, p ClientParticipant) {},
		//OnTrackSubscribed:          func(track *webrtc.TrackRemote, publication *RemoteTrackPublication, rp *RemoteParticipant) {},
		//OnTrackUnsubscribed:        func(track *webrtc.TrackRemote, publication *RemoteTrackPublication, rp *RemoteParticipant) {},
		//OnTrackSubscriptionFailed:  func(sid string, rp *RemoteParticipant) {},
		//OnTrackPublished:           func(publication *RemoteTrackPublication, rp *RemoteParticipant) {},
		//OnTrackUnpublished:         func(publication *RemoteTrackPublication, rp *RemoteParticipant) {},
		//OnDataReceived:             func(data []byte, params DataReceiveParams) {},
		//OnDataPacket:               func(data DataPacket, params DataReceiveParams) {},
		//OnTranscriptionReceived:    func(transcriptionSegments []*TranscriptionSegment, p Participant, publication TrackPublication) {},
	}
}

func (cb *ParticipantCallback) Merge(other *ParticipantCallback) {
	if other.OnLocalTrackPublished != nil {
		cb.OnLocalTrackPublished = other.OnLocalTrackPublished
	}
	if other.OnLocalTrackUnpublished != nil {
		cb.OnLocalTrackUnpublished = other.OnLocalTrackUnpublished
	}
	if other.OnTrackMuted != nil {
		cb.OnTrackMuted = other.OnTrackMuted
	}
	if other.OnTrackUnmuted != nil {
		cb.OnTrackUnmuted = other.OnTrackUnmuted
	}
	if other.OnMetadataChanged != nil {
		cb.OnMetadataChanged = other.OnMetadataChanged
	}
	if other.OnAttributesChanged != nil {
		cb.OnAttributesChanged = other.OnAttributesChanged
	}
	if other.OnIsSpeakingChanged != nil {
		cb.OnIsSpeakingChanged = other.OnIsSpeakingChanged
	}
	if other.OnConnectionQualityChanged != nil {
		cb.OnConnectionQualityChanged = other.OnConnectionQualityChanged
	}
	//if other.OnTrackSubscribed != nil {
	//	cb.OnTrackSubscribed = other.OnTrackSubscribed
	//}
	//if other.OnTrackUnsubscribed != nil {
	//	cb.OnTrackUnsubscribed = other.OnTrackUnsubscribed
	//}
	//if other.OnTrackSubscriptionFailed != nil {
	//	cb.OnTrackSubscriptionFailed = other.OnTrackSubscriptionFailed
	//}
	//if other.OnTrackPublished != nil {
	//	cb.OnTrackPublished = other.OnTrackPublished
	//}
	//if other.OnTrackUnpublished != nil {
	//	cb.OnTrackUnpublished = other.OnTrackUnpublished
	//}
	//if other.OnDataReceived != nil {
	//	cb.OnDataReceived = other.OnDataReceived
	//}
	//if other.OnDataPacket != nil {
	//	cb.OnDataPacket = other.OnDataPacket
	//}
	//if other.OnTranscriptionReceived != nil {
	//	cb.OnTranscriptionReceived = other.OnTranscriptionReceived
	//}
}
