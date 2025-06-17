package rtc

import (
	"context"
	"errors"
	"github.com/livekit/livekit-server/pkg/rtc/relay"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/livekit-server/pkg/sfu"
	"github.com/livekit/livekit-server/pkg/telemetry"
	"github.com/livekit/livekit-server/pkg/telemetry/prometheus"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils"
	"go.uber.org/atomic"
	"sync"
	"time"
)

var (
	trackRelayReconcileInterval = 1 * time.Second
	relayTrackTimeout           = iceFailedTimeoutTotal
)

// --------------------------------------------------------------------------------------

type trackRelayRequest struct {
	remoteNodeID livekit.NodeID
	trackID      livekit.TrackID
}

type RelayManagerParams struct {
	Logger        logger.Logger
	Participant   types.LocalParticipant
	TrackResolver types.MediaTrackResolver
	//OnTrackSubscribed   func(subTrack types.SubscribedTrack)
	//OnTrackUnsubscribed func(subTrack types.SubscribedTrack)
	//OnSubscriptionError func(trackID livekit.TrackID, fatal bool, err error)
	Telemetry telemetry.TelemetryService

	RelayLimitVideo, RelayLimitAudio int32

	//UseOneShotSignallingMode bool
}

// RelayManager manages a participant's node relay
type RelayManager struct {
	params                   RelayManagerParams
	logger                   logger.Logger
	lock                     sync.RWMutex
	pendingRemoveTrackRelays atomic.Int32

	relayedVideoCount, relayedAudioCount atomic.Int32

	participant       types.LocalParticipant
	relayParticipants map[livekit.NodeID]*relay.RelayParticipant
	relayTo           map[livekit.NodeID]map[livekit.TrackID]*trackRelay
	reconcileCh       chan trackRelayRequest
	closeCh           chan struct{}
	doneCh            chan struct{}

	//
	//onSubscribeStatusChanged func(publisherID livekit.ParticipantID, subscribed bool)
}

func NewRelayManager(params RelayManagerParams) *RelayManager {
	m := &RelayManager{
		params:            params,
		logger:            params.Logger,
		participant:       params.Participant,
		relayParticipants: make(map[livekit.NodeID]*relay.RelayParticipant),
		relayTo:           make(map[livekit.NodeID]map[livekit.TrackID]*trackRelay),
		reconcileCh:       make(chan trackRelayRequest, 50),
		closeCh:           make(chan struct{}),
		doneCh:            make(chan struct{}),
	}

	go m.trackRelayReconcileWorker()
	return m
}

func (m *RelayManager) isClosed() bool {
	select {
	case <-m.closeCh:
		return true
	default:
		return false
	}
}

func (m *RelayManager) AddTrackRelayToAllNodes(trackID livekit.TrackID) {
	m.lock.RLock()
	relayParticipants := make(map[livekit.NodeID]*relay.RelayParticipant)
	for nodeId, rp := range m.relayParticipants {
		relayParticipants[nodeId] = rp
	}
	m.lock.RUnlock()

	for nodeId, rp := range relayParticipants {
		if !rp.IsClosed() {
			m.addTrackRelayToNode(nodeId, trackID)
		}
	}
}

func (m *RelayManager) addTrackRelayToNode(remoteNodeID livekit.NodeID, trackID livekit.TrackID) {
	//if m.params.UseOneShotSignallingMode {
	//	m.subscribeSynchronous(trackID)
	//	return
	//}

	m.logger.Debugw("add relay track for remote node", "RemoteNodeID", remoteNodeID, "TrackID", trackID)

	t, desireChanged := m.setDesired(remoteNodeID, trackID, true)
	if t == nil {
		m.logger.Debugw("no track relay", "RemoteNodeID", remoteNodeID, "TrackID", trackID,
			"desireChanged", desireChanged)

		sLogger := m.params.Logger.WithValues(
			"trackID", trackID,
		)
		t = newTrackRelay(remoteNodeID, trackID, sLogger)
		t.setPublisher(m.participant.Identity(), m.participant.ID())

		m.lock.Lock()
		nodeTrackRelays, ok := m.relayTo[remoteNodeID]
		if !ok {
			nodeTrackRelays = make(map[livekit.TrackID]*trackRelay)
			m.relayTo[remoteNodeID] = nodeTrackRelays
		}
		m.relayTo[remoteNodeID][trackID] = t
		m.lock.Unlock()

		t, desireChanged = m.setDesired(remoteNodeID, trackID, true)
	}
	if desireChanged {
		m.logger.Debugw("creating relay track for remote node", "RemoteNodeID", remoteNodeID)
	}

	// always reconcile, since SubscribeToTrack could be called when the track is ready
	m.queueTrackRelayReconcile(remoteNodeID, trackID)
}

func (m *RelayManager) RemoveTrackRelayFromNode(remoteNodeID livekit.NodeID, trackID livekit.TrackID) {
	//if m.params.UseOneShotSignallingMode {
	//	m.unsubscribeSynchronous(trackID)
	//	return
	//}

	t, desireChanged := m.setDesired(remoteNodeID, trackID, false)
	if t == nil || !desireChanged {
		return
	}

	t.logger.Debugw("removing track from remote node", "RemoteNodeID", remoteNodeID)
	m.queueTrackRelayReconcile(remoteNodeID, trackID)
}

func (m *RelayManager) canRelay() bool {
	p := m.participant
	if m.isClosed() || p.IsClosed() || p.IsDisconnected() || p.IsRemoteRelay() {
		return false
	}
	return true
}

func (m *RelayManager) GetRelayParticipant(nodeID livekit.NodeID) *relay.RelayParticipant {
	m.lock.RLock()
	defer m.lock.RUnlock()

	r, ok := m.relayParticipants[nodeID]
	if !ok {
		return nil
	}

	// update subscribed quality to trigger sending of simulcast video layers
	//go func() {
	//	mediaTracks := m.participant.GetPublishedTracks()
	//	for _, track := range mediaTracks {
	//		trackInfo := track.ToProto()
	//		if track.IsSimulcast() && len(trackInfo.Layers) > 0 {
	//			relayedQualities := make([]types.SubscribedCodecQuality, 0)
	//			for _, layer := range trackInfo.Layers {
	//				relayedQuality := types.SubscribedCodecQuality{
	//					CodecMime: mime.NormalizeMimeType(trackInfo.MimeType),
	//					Quality:   layer.Quality,
	//				}
	//				relayedQualities = append(relayedQualities, relayedQuality)
	//			}
	//			err := m.participant.UpdateSubscribedQuality(nodeID, track.ID(), relayedQualities)
	//			if err != nil {
	//				m.logger.Errorw("update subscriber quality failed", err,
	//					"destNodeID", nodeID, "trackID", track.ID,
	//					"relayedQualities", relayedQualities)
	//			}
	//		}
	//	}
	//}()

	return r
}

func (m *RelayManager) AddRelayParticipantToNode(nodeID livekit.NodeID, rp *relay.RelayParticipant) error {

	m.logger.Debugw("RelayManager: add relay participant", "RemoteNodeID", nodeID,
		"ParticipantID", rp.SID(),
		"ParticipantName", rp.Name())

	m.lock.RLock()
	_, ok := m.relayParticipants[nodeID]
	m.lock.RUnlock()

	if !ok {
		m.logger.Debugw("RelayManager: new remote node", "RemoteNodeID", nodeID,
			"ParticipantID", rp.SID(),
			"ParticipantName", rp.Name())

		m.lock.Lock()
		m.relayParticipants[nodeID] = rp
		m.lock.Unlock()

		rp.AddOnClose(func() {
			m.lock.RLock()
			tracks := m.participant.GetPublishedTracks()
			m.lock.RUnlock()

			for _, track := range tracks {
				m.RemoveTrackRelayFromNode(nodeID, track.ID())
			}
		})
	}

	m.lock.RLock()
	tracks := m.participant.GetPublishedTracks()
	m.lock.RUnlock()

	m.logger.Debugw("RelayManager: participant tracks", "RemoteNodeID", nodeID,
		"ParticipantID", rp.SID(), "ParticipantName", rp.Name(), "PublishedTracksNum", len(tracks))

	for _, track := range tracks {
		// TODO: remove the kind is VIDEO check
		//if track.Kind() == livekit.TrackType_VIDEO {
		//	m.addTrackRelayToNode(nodeID, track.ID())
		//}
		m.addTrackRelayToNode(nodeID, track.ID())
	}
	return nil
}

func (m *RelayManager) RemoveRelayParticipantFromNode(nodeID livekit.NodeID) (*relay.RelayParticipant, error) {
	m.lock.RLock()
	tracks := m.participant.GetPublishedTracks()
	m.lock.RUnlock()

	for _, track := range tracks {
		m.RemoveTrackRelayFromNode(nodeID, track.ID())
	}
	return nil, nil
}

func (m *RelayManager) Close() {
	m.lock.Lock()
	if m.isClosed() {
		m.lock.Unlock()
		return
	}
	close(m.closeCh)
	m.lock.Unlock()

	// wait for trackRelayConcileWorker routine done
	<-m.doneCh

	//// close all relay participants
	//subTracks := m.GetSubscribedTracks()
	//downTracksToClose := make([]*sfu.DownTrack, 0, len(subTracks))
	//for _, st := range subTracks {
	//	m.setDesired(st.ID(), false)
	//	dt := st.DownTrack()
	//	// nil check exists primarily for tests
	//	if dt != nil {
	//		downTracksToClose = append(downTracksToClose, st.DownTrack())
	//	}
	//}
	//
	//if isExpectedToResume {
	//	for _, dt := range downTracksToClose {
	//		dt.CloseWithFlush(false)
	//	}
	//} else {
	//	// flush blocks, so execute in parallel
	//	for _, dt := range downTracksToClose {
	//		go dt.CloseWithFlush(true)
	//	}
	//}
}

func (m *RelayManager) canReconcile() bool {
	p := m.params.Participant
	if m.isClosed() || /*p.IsClosed() || p.IsDisconnected() ||*/ !p.CanRelay() {
		return false
	}
	return true
}

func (m *RelayManager) setDesired(remoteNodeID livekit.NodeID, trackID livekit.TrackID, desired bool) (*trackRelay, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	nodeTrackRelays, ok := m.relayTo[remoteNodeID]
	if !ok {
		return nil, false
	}

	r, ok := nodeTrackRelays[trackID]
	if !ok {
		return nil, false
	}

	m.logger.Debugw("set track relay desired!", "RemoteNodeID", remoteNodeID, "TrackID", trackID,
		"Desired", desired)
	return r, r.setDesired(desired)
}

func (m *RelayManager) reconcileTrackRelays() {
	var needsToReconcile []*trackRelay
	m.lock.RLock()
	for _, nodeTracks := range m.relayTo {
		for _, trackRelay := range nodeTracks {
			if trackRelay.needsRelay() || trackRelay.needsRemoveRelay() /*|| trackRelay.needsBind()*/ || trackRelay.needsCleanup() {
				needsToReconcile = append(needsToReconcile, trackRelay)
			}
		}
	}
	m.lock.RUnlock()

	for _, t := range needsToReconcile {
		m.reconcileTrackRelay(t)
	}
}

func (m *RelayManager) reconcileTrackRelay(t *trackRelay) {
	if !m.canReconcile() {
		return
	}

	p := m.params.Participant
	if p.IsClosed() || p.IsDisconnected() {
		t.setDesired(false)
	}

	if t.needsRelay() {

		m.logger.Debugw("track needs relay",
			"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

		if m.pendingRemoveTrackRelays.Load() != 0 && t.durationSinceStart() < maxUnsubscribeWait {

			m.logger.Debugw("pending remove track relays exist, will wait",
				"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID,
				"pendingRemoveTrackRelays", m.pendingRemoveTrackRelays.Load())

			// enqueue this in a bit, after pending unsubscribes are complete
			go func() {
				time.Sleep(time.Duration(sfu.RTPBlankFramesCloseSeconds * float32(time.Second)))
				m.queueTrackRelayReconcile(t.remoteNodeID, t.trackID)
			}()
			return
		}

		numAttempts := t.getNumAttempts()
		if numAttempts == 0 {
			m.params.Telemetry.TrackRelayRequested(
				context.Background(),
				m.params.Participant.ID(),
				&livekit.TrackInfo{
					Sid: string(t.trackID),
				},
			)
		}

		m.logger.Debugw("add track relay",
			"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

		if err := m.addTrackRelay(t); err != nil {

			m.logger.Errorw("add track relay failed", err,
				"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

			t.recordAttempt(false)

			switch err {
			case ErrNoReceiver, ErrNotOpen:
				// these are errors that are out of our control, so we'll keep trying
				// - ErrNoReceiver: Track is in the process of closing (another local track published to the same instance)
				// - ErrNotOpen: Track is closing or already closed
				// - ErrRelayLimitExceeded: the participant have reached the limit of subscriptions, wait for the other subscription to be unsubscribed
				// We'll still log an event to reflect this in telemetry since it's been too long
				if t.durationSinceStart() > subscriptionTimeout {
					t.logger.Errorw("create track relay to remote node failed!", err)
					t.maybeRecordError(m.params.Telemetry, m.params.Participant.ID(), err, true)
				}
			case ErrTrackNotFound:
				// source track was never published or closed
				// if after timeout we'd unsubscribe from it.
				// this is the *only* case we'd change desired state
				if t.durationSinceStart() > notFoundTimeout {
					t.maybeRecordError(m.params.Telemetry, m.params.Participant.ID(), err, true)
					t.logger.Infow("source track does not exist, will remove track relay", "error", err)
					t.setDesired(false)
					m.queueTrackRelayReconcile(t.remoteNodeID, t.trackID)
					//m.params.OnSubscriptionError(s.trackID, false, err)
				}
			default:
				// all other errors
				if t.durationSinceStart() > relayTrackTimeout {
					t.logger.Warnw("failed to relay track to remote node, triggering error handler", err,
						"attempt", numAttempts,
					)
					t.maybeRecordError(m.params.Telemetry, m.params.Participant.ID(), err, false)
					//m.params.OnSubscriptionError(s.trackID, true, err)
				} else {
					t.logger.Debugw("failed to relay track to remote node, retrying",
						"error", err,
						"attempt", numAttempts,
					)
				}
			}
		} else {
			m.logger.Debugw("add track relay success!",
				"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

			t.recordAttempt(true)
		}

		return
	}

	if t.needsRemoveRelay() {

		m.logger.Debugw("needs remove track relay",
			"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

		if err := m.removeTrackRelay(t); err != nil {
			if errors.Is(err, ErrTrackNotFound) {

				m.logger.Errorw("track relay not found when removing", err,
					"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

				t.lock.Lock()
				t.relayedTrack = nil
				t.lock.Unlock()

				t.recordAttempt(true)
			} else {

				m.logger.Errorw("remove track relay failed", err,
					"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

				t.recordAttempt(false)
			}
		} else {

			m.logger.Debugw("remove track relay success!",
				"RemoteNodeID", t.remoteNodeID, "TrackID", t.trackID)

			t.lock.Lock()
			t.relayedTrack = nil
			t.lock.Unlock()

			t.recordAttempt(true)
		}
		return
	}

	m.lock.Lock()
	if t.needsCleanup() {

		t.logger.Debugw("cleanup track relay")

		trackRelayMap := m.relayTo[t.remoteNodeID]
		if trackRelayMap != nil && len(trackRelayMap) > 0 {

			t.logger.Debugw("remove track relay for track",
				"destNodeID", t.remoteNodeID, "trackID", t.trackID)

			delete(trackRelayMap, t.trackID)
			m.relayTo[t.remoteNodeID] = trackRelayMap
		} else {

			t.logger.Debugw("no more track relays exist",
				"destNodeID", t.remoteNodeID, "trackID", t.trackID)

			delete(m.relayParticipants, t.remoteNodeID)
		}
	}
	m.lock.Unlock()
}

// trigger an immediate reconciliation, when trackID is empty, will reconcile all subscriptions
func (m *RelayManager) queueTrackRelayReconcile(remoteNodeID livekit.NodeID, trackID livekit.TrackID) {
	req := trackRelayRequest{
		remoteNodeID: remoteNodeID,
		trackID:      trackID,
	}

	select {
	case m.reconcileCh <- req:
	default:
		// queue is full, will reconcile based on timer
	}
}

func (m *RelayManager) trackRelayReconcileWorker() {
	reconcileTicker := time.NewTicker(trackRelayReconcileInterval)
	defer reconcileTicker.Stop()
	defer close(m.doneCh)

	for {
		select {
		case <-m.closeCh:
			return
		case <-reconcileTicker.C:
			m.reconcileTrackRelays()
		case req := <-m.reconcileCh:
			m.lock.Lock()
			trackRelay := m.relayTo[req.remoteNodeID][req.trackID]
			m.lock.Unlock()
			if trackRelay != nil {
				m.reconcileTrackRelay(trackRelay)
			} else {
				m.reconcileTrackRelays()
			}
		}
	}
}

func (m *RelayManager) hasCapacityForRelay(kind livekit.TrackType) bool {
	switch kind {
	case livekit.TrackType_VIDEO:
		if m.params.RelayLimitVideo > 0 && m.relayedVideoCount.Load() >= m.params.RelayLimitVideo {
			return false
		}

	case livekit.TrackType_AUDIO:
		if m.params.RelayLimitAudio > 0 && m.relayedAudioCount.Load() >= m.params.RelayLimitAudio {
			return false
		}
	}
	return true
}

func (m *RelayManager) addTrackRelay(t *trackRelay) error {
	t.logger.Debugw("relay manager: executing addTrackRelay")

	if !m.params.Participant.CanRelay() {
		return relay.ErrTrackCannotBeRelayed
	}

	if kind, ok := t.getKind(); ok && !m.hasCapacityForRelay(kind) {
		return relay.ErrRelayLimitExceeded
	}

	trackID := t.trackID
	rp := m.relayParticipants[t.remoteNodeID]
	if rp == nil {
		m.logger.Errorw("relay manager: relay channel to remote not created!", relay.ErrNoRelayToRemoteNode,
			"RemoteNode", t.remoteNodeID, "TrackID", trackID)
		return relay.ErrNoRelayToRemoteNode
	}

	if rp.State() == livekit.ParticipantInfo_JOINING {
		m.logger.Errorw("relay manager: relay participant has not joined yet, will retry later",
			relay.ErrParticipantNotReadyForRelay,
			"RemoteNode", t.remoteNodeID, "TrackID", trackID)
		return relay.ErrParticipantNotReadyForRelay
	} else if rp.State() == livekit.ParticipantInfo_DISCONNECTED {
		m.logger.Errorw("relay manager: relay participant is disconnected!",
			relay.ErrParticipantRelaySignalBroken,
			"RemoteNode", t.remoteNodeID, "TrackID", trackID)
		return relay.ErrParticipantRelaySignalBroken
	}

	res := m.params.TrackResolver(m.params.Participant, trackID)
	t.logger.Debugw("relay manager: resolved track", "result", res)

	if res.TrackChangedNotifier != nil && t.setChangedNotifier(res.TrackChangedNotifier) {
		// set callback only when we haven't done it before
		// we set the observer before checking for existence of track, so that we may get notified
		// when the track becomes available
		res.TrackChangedNotifier.AddObserver(string(m.params.Participant.ID()), func() {
			m.queueTrackRelayReconcile(t.remoteNodeID, trackID)
		})
	}
	if res.TrackRemovedNotifier != nil && t.setRemovedNotifier(res.TrackRemovedNotifier) {
		res.TrackRemovedNotifier.AddObserver(string(m.params.Participant.ID()), func() {
			// re-resolve the track in case the same track had been re-published
			res := m.params.TrackResolver(m.params.Participant, trackID)
			if res.Track != nil {
				// do not remove relay, track is still available
				return
			}
			m.handleSourceTrackRemoved(t.remoteNodeID, trackID)
		})
	}

	track := res.Track
	if track == nil {
		return ErrTrackNotFound
	}
	t.trySetKind(track.Kind())
	if !m.hasCapacityForRelay(track.Kind()) {
		return relay.ErrRelayLimitExceeded
	}

	t.setPublisher(res.PublisherIdentity, res.PublisherID)

	//permChanged := s.setHasPermission(res.HasPermission)
	//if permChanged {
	//	m.params.Participant.SubscriptionPermissionUpdate(s.getPublisherID(), trackID, res.HasPermission)
	//}
	//if !res.HasPermission {
	//	return ErrNoTrackPermission
	//}

	relayedTrack, err := track.AddRelay(rp)
	if err != nil && !errors.Is(err, errAlreadyRelayed) {
		// ignore error(s): already subscribed
		if !utils.ErrorIsOneOf(err, ErrNoReceiver) {
			// as track resolution could take some time, not logging errors due to waiting for track resolution
			m.params.Logger.Warnw("add relay failed", err,
				"destNodeID", rp.DestNodeID(), "trackID", trackID)
		}
		return err
	}

	if errors.Is(err, errAlreadyRelayed) {
		m.params.Logger.Debugw(
			"already relayed to track",
			"destNodeID", rp.DestNodeID(),
			"trackID", trackID,
			"subscribedAudioCount", m.relayedAudioCount.Load(),
			"subscribedVideoCount", m.relayedVideoCount.Load(),
		)
	}
	if err == nil && relayedTrack != nil { // relayedTrack could be nil if already subscribed
		relayedTrack.OnClose(func(isExpectedToResume bool) {
			m.handleRelayedTrackClose(t, isExpectedToResume)
		})
		//subTrack.AddOnBind(func(err error) {
		//	if err != nil {
		//		t.logger.Infow("failed to bind track", "err", err)
		//		t.maybeRecordError(m.params.Telemetry, m.params.Participant.ID(), err, true)
		//		m.UnsubscribeFromTrack(trackID)
		//		m.params.OnSubscriptionError(trackID, false, err)
		//		return
		//	}
		//	t.setBound()
		//	t.maybeRecordSuccess(m.params.Telemetry, m.params.Participant.ID())
		//})
		t.setRelayedTrack(relayedTrack)
		t.maybeRecordSuccess(m.params.Telemetry, m.params.Participant.ID())

		switch track.Kind() {
		case livekit.TrackType_VIDEO:
			m.relayedVideoCount.Inc()
		case livekit.TrackType_AUDIO:
			m.relayedAudioCount.Inc()
		}

		// update subscribed quality to trigger sending of simulcast video layers
		if relayedTrack.IsSimulcast() {
			relayedQualities := relayedTrack.GetRelayedQualities()
			err = m.participant.UpdateSubscribedQuality(t.remoteNodeID, trackID, relayedQualities)
			if err != nil {
				m.logger.Errorw("update subscriber quality failed", err,
					"destNodeID", t.remoteNodeID, "trackID", trackID,
					"relayedQualities", relayedQualities)
			}
		}

		//	if subTrack.NeedsNegotiation() {
		//		m.params.Participant.Negotiate(false)
		//	}
		//
		//	go m.params.OnTrackSubscribed(subTrack)
		//
		//	m.params.Logger.Debugw(
		//		"subscribed to track",
		//		"trackID", trackID,
		//		"subscribedAudioCount", m.relayedAudioCount.Load(),
		//		"subscribedVideoCount", m.relayedVideoCount.Load(),
		//	)
	}

	// add mark the participant as someone we've subscribed to
	//firstSubscribe := false
	//publisherID := t.getPublisherID()
	//m.lock.Lock()
	//pTracks := m.subscribedTo[publisherID]
	//changedCB := m.onSubscribeStatusChanged
	//if pTracks == nil {
	//	pTracks = make(map[livekit.TrackID]struct{})
	//	m.subscribedTo[publisherID] = pTracks
	//	firstSubscribe = true
	//}
	//pTracks[trackID] = struct{}{}
	//m.lock.Unlock()
	//
	//if changedCB != nil && firstSubscribe {
	//	changedCB(publisherID, true)
	//}

	return nil
}

func (m *RelayManager) removeTrackRelay(t *trackRelay) error {
	m.logger.Debugw("relay manager: executing removeTrackRelay")

	trackID := t.trackID
	rp := m.relayParticipants[t.remoteNodeID]
	if rp == nil {
		m.logger.Errorw("relay manager: relay channel to remote is lost!", relay.ErrNoRelayToRemoteNode,
			"RemoteNode", t.remoteNodeID, "TrackID", trackID)

		// TODO: remove trackRelay without relay participant client

		return nil
	}
	err := rp.UnpublishTrack(t.relayedTrack.GetLocalPublication().SID())
	if err != nil {
		t.logger.Errorw("send unpublish track request failed", err, "trackID", trackID)
	}

	// TODO: should check if m.params.Participant exists before the following code

	res := m.params.TrackResolver(m.params.Participant, trackID)
	t.logger.Debugw("relay manager: resolved track", "result", res)

	if res.TrackChangedNotifier != nil && t.setChangedNotifier(res.TrackChangedNotifier) {
		// set callback only when we haven't done it before
		// we set the observer before checking for existence of track, so that we may get notified
		// when the track becomes available
		res.TrackChangedNotifier.RemoveObserver(string(m.params.Participant.ID()))
	}
	if res.TrackRemovedNotifier != nil && t.setRemovedNotifier(res.TrackRemovedNotifier) {
		res.TrackRemovedNotifier.RemoveObserver(string(m.params.Participant.ID()))
	}

	track := res.Track
	if track == nil {
		return ErrTrackNotFound
	}
	m.pendingRemoveTrackRelays.Inc()

	go func() {
		defer m.pendingRemoveTrackRelays.Dec()
		track.RemoveRelay(t.remoteNodeID, false)

		t.lock.Lock()
		t.relayedTrack = nil
		t.lock.Unlock()
	}()

	return nil
}

func (m *RelayManager) handleSourceTrackRemoved(remoteNodeID livekit.NodeID, trackID livekit.TrackID) {
	m.lock.Lock()
	nodeTracks := m.relayTo[remoteNodeID]
	if nodeTracks == nil || len(nodeTracks) == 0 {
		m.lock.Unlock()
		return
	}
	relayTrack := nodeTracks[trackID]
	m.lock.Unlock()

	if relayTrack != nil {
		relayTrack.handleSourceTrackRemoved()
	}
}

func (m *RelayManager) handleRelayedTrackClose(t *trackRelay, isExpectedToResume bool) {
	// TODO: to be done

	t.logger.Debugw(
		"relayed track closed",
		"isExpectedToResume", isExpectedToResume,
	)
	//wasBound := t.isBound()
	relayedTrack := t.getRelayedTrack()
	if relayedTrack == nil {
		return
	}
	//m.setRelayedTrack(nil)

	//var relieveFromLimits bool
	//switch relayedTrack.MediaTrack().Kind() {
	//case livekit.TrackType_VIDEO:
	//	videoCount := m.relayedVideoCount.Dec()
	//	relieveFromLimits = m.params.RelayLimitVideo > 0 && videoCount == m.params.RelayLimitVideo-1
	//case livekit.TrackType_AUDIO:
	//	audioCount := m.relayedAudioCount.Dec()
	//	relieveFromLimits = m.params.RelayLimitAudio > 0 && audioCount == m.params.RelayLimitAudio-1
	//}

	//// remove from subscribedTo
	//publisherID := s.getPublisherID()
	//lastSubscription := false
	//m.lock.Lock()
	//changedCB := m.onSubscribeStatusChanged
	//pTracks := m.subscribedTo[publisherID]
	//if pTracks != nil {
	//	delete(pTracks, s.trackID)
	//	if len(pTracks) == 0 {
	//		delete(m.subscribedTo, publisherID)
	//		lastSubscription = true
	//	}
	//}
	//m.lock.Unlock()
	//if changedCB != nil && lastSubscription {
	//	go changedCB(publisherID, false)
	//}
	//
	//go m.params.OnTrackUnsubscribed(subTrack)
	//
	//// trigger to decrement unsubscribed counter as long as track has been bound
	//// Only log an analytics event when
	//// * the participant isn't closing
	//// * it's not a migration
	//if wasBound {

	m.params.Telemetry.TrackRelayRemoved(
		context.Background(),
		m.params.Participant.ID(),
		&livekit.TrackInfo{Sid: string(t.trackID), Type: relayedTrack.MediaTrack().Kind()},
		!isExpectedToResume,
	)

	//dt := relayedTrack.DownTrack()
	//if dt != nil {
	//	stats := dt.GetTrackStats()
	//	if stats != nil {
	//		m.params.Telemetry.TrackSubscribeRTPStats(
	//			context.Background(),
	//			m.params.Participant.ID(),
	//			t.trackID,
	//			dt.Mime(),
	//			stats,
	//		)
	//	}
	//}

	//}

	//if !isExpectedToResume {
	//	sender := relayedTrack.RTPSender()
	//	if sender != nil {
	//		t.logger.Debugw("removing PeerConnection track",
	//			"kind", relayedTrack.MediaTrack().Kind(),
	//		)
	//
	//		if err := m.params.Participant.RemoveTrackLocal(sender); err != nil {
	//			if _, ok := err.(*rtcerr.InvalidStateError); !ok {
	//				// most of these are safe to ignore, since the track state might have already
	//				// been set to Inactive
	//				m.params.Logger.Debugw("could not remove remoteTrack from forwarder",
	//					"error", err,
	//					"publisher", relayedTrack.PublisherIdentity(),
	//					"publisherID", relayedTrack.PublisherID(),
	//				)
	//			}
	//		}
	//	}
	//
	//	m.params.Participant.Negotiate(false)
	//} else {
	//	timeNow := time.Now()
	//	t.relayAt.Store(&timeNow)
	//}

	//if !m.params.UseOneShotSignallingMode {
	//	if relieveFromLimits {
	//		m.queueReconcile(trackIDForReconcileSubscriptions)
	//	} else {
	//		m.queueReconcile(s.trackID)
	//	}
	//}
}

// --------------------------------------------------------------------------------------

type trackRelay struct {
	logger       logger.Logger
	remoteNodeID livekit.NodeID
	trackID      livekit.TrackID

	lock                     sync.RWMutex
	desired                  bool
	publisherID              livekit.ParticipantID
	publisherIdentity        livekit.ParticipantIdentity
	settings                 *livekit.UpdateTrackSettings
	changedNotifier          types.ChangeNotifier
	removedNotifier          types.ChangeNotifier
	hasPermissionInitialized bool
	hasPermission            bool
	relayedTrack             types.RelayedTrack
	//relayedCodecQualities      []types.SubscribedCodecQuality

	eventSent   atomic.Bool
	numAttempts atomic.Int32
	bound       bool
	kind        atomic.Pointer[livekit.TrackType]

	// the later of when subscription was requested OR when the first failure was encountered OR when permission is granted
	// this timestamp determines when failures are reported
	relayStartedAt atomic.Pointer[time.Time]

	// the timestamp when the subscription was started, will be reset when downtrack is closed with expected resume
	relayAt           atomic.Pointer[time.Time]
	succRecordCounter atomic.Int32
}

func newTrackRelay(remoteNoteID livekit.NodeID, trackID livekit.TrackID, logger logger.Logger) *trackRelay {
	s := &trackRelay{
		logger:       logger,
		remoteNodeID: remoteNoteID,
		trackID:      trackID,
	}
	t := time.Now()
	s.relayAt.Store(&t)
	return s
}

func (t *trackRelay) setPublisher(publisherIdentity livekit.ParticipantIdentity, publisherID livekit.ParticipantID) {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.publisherID = publisherID
	t.publisherIdentity = publisherIdentity
}

func (t *trackRelay) getPublisherID() livekit.ParticipantID {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.publisherID
}

func (t *trackRelay) setDesired(desired bool) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	if desired {
		// as long as user explicitly set it to desired
		// we'll reset the timer so it has sufficient time to reconcile
		timeNow := time.Now()
		t.relayStartedAt.Store(&timeNow)
		t.relayAt.Store(&timeNow)
	}

	if t.desired == desired {
		return false
	}
	t.desired = desired

	// when no longer desired, we no longer care about change notifications
	if desired {
		// reset attempts
		t.numAttempts.Store(0)
	} else {
		t.setChangedNotifierLocked(nil)
		t.setRemovedNotifierLocked(nil)
	}
	return true
}

func (t *trackRelay) isDesired() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.desired
}

func (t *trackRelay) setRelayedTrack(track types.RelayedTrack) {
	t.lock.Lock()
	//oldTrack := t.relayTrack
	t.relayedTrack = track
	t.bound = false
	t.lock.Unlock()

	//if oldTrack != nil {
	//	oldTrack.OnClose(nil)
	//}
}

func (t *trackRelay) trySetKind(kind livekit.TrackType) {
	t.kind.CompareAndSwap(nil, &kind)
}

func (t *trackRelay) getKind() (livekit.TrackType, bool) {
	kind := t.kind.Load()
	if kind == nil {
		return livekit.TrackType_AUDIO, false
	}
	return *kind, true
}

func (t *trackRelay) getRelayedTrack() types.RelayedTrack {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.relayedTrack
}

func (t *trackRelay) setChangedNotifier(notifier types.ChangeNotifier) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.setChangedNotifierLocked(notifier)
}

func (t *trackRelay) setChangedNotifierLocked(notifier types.ChangeNotifier) bool {
	if t.changedNotifier == notifier {
		return false
	}

	existing := t.changedNotifier
	t.changedNotifier = notifier

	if existing != nil {
		go existing.RemoveObserver(string(t.remoteNodeID))
	}
	return true
}

func (t *trackRelay) setRemovedNotifier(notifier types.ChangeNotifier) bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.setRemovedNotifierLocked(notifier)
}

func (t *trackRelay) setRemovedNotifierLocked(notifier types.ChangeNotifier) bool {
	if t.removedNotifier == notifier {
		return false
	}

	existing := t.removedNotifier
	t.removedNotifier = notifier

	if existing != nil {
		go existing.RemoveObserver(string(t.remoteNodeID))
	}
	return true
}

func (t *trackRelay) recordAttempt(success bool) {
	if !success {
		if t.numAttempts.Load() == 0 {
			// on first failure, we'd want to start the timer
			timeNow := time.Now()
			t.relayStartedAt.Store(&timeNow)
		}
		t.numAttempts.Add(1)
	} else {
		t.numAttempts.Store(0)
	}
}

func (t *trackRelay) getNumAttempts() int32 {
	return t.numAttempts.Load()
}

func (t *trackRelay) handleSourceTrackRemoved() {
	t.lock.Lock()
	defer t.lock.Unlock()
	startedAt := t.relayStartedAt.Load()
	if startedAt == nil || time.Since(*startedAt) < trackRemoveGracePeriod {
		// to prevent race conditions, if we've recently been asked to relay a track
		// ignore when source was removed. reconciler will take care of it eventually
		// this would address the case when a track was unpublished and republished immediately
		// it's possible for another caller to call setDesired(true) for the republished track before
		// handleSourceTrackRemoved is called on the previously unpublished track
		return
	}

	// source track removed, we would unsubscribe
	t.logger.Debugw("removing relay for track since source track was removed")
	t.desired = false

	t.setChangedNotifierLocked(nil)
	t.setRemovedNotifierLocked(nil)
}

func (s *trackRelay) maybeRecordError(ts telemetry.TelemetryService, pID livekit.ParticipantID, err error, isUserError bool) {
	if s.eventSent.Swap(true) {
		return
	}

	ts.TrackRelayFailed(context.Background(), pID, s.trackID, err, isUserError)
}

func (s *trackRelay) maybeRecordSuccess(ts telemetry.TelemetryService, pID livekit.ParticipantID) {
	relayedTrack := s.getRelayedTrack()
	if relayedTrack == nil {
		return
	}
	mediaTrack := relayedTrack.MediaTrack()
	if mediaTrack == nil {
		return
	}

	d := time.Since(*s.relayAt.Load())
	s.logger.Debugw("track relayed", "cost", d.Milliseconds())
	prometheus.RecordRelayTime(mediaTrack.Source(), mediaTrack.Kind(), d,
		livekit.ClientInfo_GO, livekit.ParticipantInfo_STANDARD, int(s.succRecordCounter.Inc()))

	eventSent := s.eventSent.Swap(true)

	pi := &livekit.ParticipantInfo{
		Identity: string(relayedTrack.PublisherIdentity()),
		Sid:      string(relayedTrack.PublisherID()),
	}
	ts.TrackRelayed(context.Background(), pID, mediaTrack.ToProto(), pi, !eventSent)
}

func (t *trackRelay) durationSinceStart() time.Duration {
	startAt := t.relayStartedAt.Load()
	if startAt == nil {
		return 0
	}
	return time.Since(*startAt)
}

func (t *trackRelay) needsRelay() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.desired && t.relayedTrack == nil
}

func (t *trackRelay) needsRemoveRelay() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return !t.desired && t.relayedTrack != nil
}

func (t *trackRelay) needsBind() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.desired && t.relayedTrack != nil && !t.bound
}

func (t *trackRelay) needsCleanup() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return !t.desired && t.relayedTrack == nil
}
