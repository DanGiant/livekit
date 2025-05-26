package rtc

import (
	"github.com/livekit/livekit-server/pkg/rtc/relay"
	"github.com/livekit/livekit-server/pkg/rtc/types"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"go.uber.org/atomic"
	"sync"
	"time"
)

var (
	remoteRoomReconcileInterval = 1 * time.Second
)

// --------------------------------------------------------------------------------------

type RemoteRoomManager interface {
	CreateRelayParticipantToNode(participantID livekit.ParticipantID, remoteNode livekit.NodeID, roomName livekit.RoomName) (*relay.RelayParticipant, error)
}

// --------------------------------------------------------------------------------------

type RemoteRoomParams struct {
	Logger       logger.Logger
	RoomManager  RemoteRoomManager
	Room         types.Room
	RemoteNodeID livekit.NodeID
}

type RemoteRoom struct {
	params               RemoteRoomParams
	logger               logger.Logger
	lock                 sync.RWMutex
	roomManager          RemoteRoomManager
	room                 types.Room
	remoteNodeID         livekit.NodeID
	participantRelayings map[livekit.ParticipantID]*participantRelaying
	reconcileCh          chan livekit.ParticipantID
	closeCh              chan struct{}
	doneCh               chan struct{}

	relayParticipants map[livekit.ParticipantID]*relay.RelayParticipant

	onParticipantStateChange func(p types.LocalParticipant, state livekit.ParticipantInfo_State)
	onClose                  []func()
}

func NewRemoteRoom(params RemoteRoomParams) *RemoteRoom {
	m := &RemoteRoom{
		params:               params,
		logger:               params.Logger,
		roomManager:          params.RoomManager,
		room:                 params.Room,
		remoteNodeID:         params.RemoteNodeID,
		participantRelayings: make(map[livekit.ParticipantID]*participantRelaying),
		reconcileCh:          make(chan livekit.ParticipantID, 50),
		closeCh:              make(chan struct{}),
		doneCh:               make(chan struct{}),
		relayParticipants:    make(map[livekit.ParticipantID]*relay.RelayParticipant),
		onClose:              make([]func(), 0),
	}

	go m.reconcileWorker()
	return m
}

func (m *RemoteRoom) GetRemoteNodeID() livekit.NodeID {
	return m.remoteNodeID
}

func (m *RemoteRoom) GetRelayParticipant(participantID livekit.ParticipantID) *relay.RelayParticipant {
	m.lock.RLock()
	defer m.lock.RUnlock()
	relaying, ok := m.participantRelayings[participantID]
	if !ok {
		return nil
	} else {
		return relaying.relayParticipant
	}
}

func (m *RemoteRoom) OnParticipantStateChange(callback func(p types.LocalParticipant, state livekit.ParticipantInfo_State)) {
	m.lock.Lock()
	m.onParticipantStateChange = callback
	m.lock.Unlock()
}

func (m *RemoteRoom) GetOnParticipantStateChange() func(p types.LocalParticipant, state livekit.ParticipantInfo_State) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.onParticipantStateChange
}

func (m *RemoteRoom) AddParticipant(participantID livekit.ParticipantID) {
	relaying, desireChanged := m.setDesired(participantID, true)
	if relaying == nil {
		rLogger := m.params.Logger.WithValues(
			"ParticipantID", participantID,
		)
		relaying = newParticipantRelaying(participantID, m.remoteNodeID, rLogger)

		m.lock.Lock()
		m.participantRelayings[participantID] = relaying
		m.lock.Unlock()

		relaying, desireChanged = m.setDesired(participantID, true)
	}
	if desireChanged {
		relaying.logger.Debugw("begin a participant relay to remote node",
			"remoteNodeID", m.remoteNodeID, "RoomName", m.room.Name())
	}

	// always reconcile, since SubscribeToTrack could be called when the track is ready
	m.queueReconcile(participantID)
}

func (m *RemoteRoom) RemoveParticipant(participantID livekit.ParticipantID) {
	relaying, desireChanged := m.setDesired(participantID, false)
	if relaying == nil || !desireChanged {
		return
	}

	relaying.logger.Debugw("end a participant relay to remote node",
		"remoteNodeID", m.remoteNodeID, "RoomName", m.room.Name())
	m.queueReconcile(participantID)
}

// trigger an immediate reconciliation, when participant is empty, will reconcile all
func (m *RemoteRoom) queueReconcile(participantID livekit.ParticipantID) {
	select {
	case m.reconcileCh <- participantID:
	default:
		// queue is full, will reconcile based on timer
	}
}

func (m *RemoteRoom) reconcileWorker() {
	reconcileTicker := time.NewTicker(remoteRoomReconcileInterval)
	defer reconcileTicker.Stop()
	defer close(m.doneCh)

	for {
		select {
		case <-m.closeCh:
			return
		case <-reconcileTicker.C:
			m.reconcileParticipantRelayings()
		case pid := <-m.reconcileCh:
			m.lock.Lock()
			r := m.participantRelayings[pid]
			m.lock.Unlock()
			if r != nil {
				m.reconcileParticipantRelaying(r)
			} else {
				m.reconcileParticipantRelayings()
			}
		}
	}
}

func (m *RemoteRoom) isClosed() bool {
	select {
	case <-m.closeCh:
		return true
	default:
		return false
	}
}

func (m *RemoteRoom) AddOnClose(f func()) {
	if f == nil {
		return
	}

	m.lock.Lock()
	m.onClose = append(m.onClose, f)
	m.lock.Unlock()
}

func (m *RemoteRoom) Close() {
	m.lock.Lock()
	if m.isClosed() {
		m.lock.Unlock()
		return
	}

	// stop reconcileWorker routine
	close(m.closeCh)
	m.lock.Unlock()

	// wait for reconcileWorker routine stop
	<-m.doneCh

	m.lock.Lock()
	onclose := m.onClose
	m.onClose = make([]func(), 0)
	m.lock.Unlock()

	for _, f := range onclose {
		f()
	}

	// close all relay participants
	//relayParticipants := m.GetRelayParticipants()
	//downTracksToClose := make([]*sfu.DownTrack, 0, len(relayParticipants))
	//for _, rp := range relayParticipants {
	//	m.setDesired(rp.ID(), false)
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

func (m *RemoteRoom) setDesired(participantID livekit.ParticipantID, desired bool) (*participantRelaying, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	relaying, ok := m.participantRelayings[participantID]
	if !ok {
		return nil, false
	}

	return relaying, relaying.setDesired(desired)
}

func (m *RemoteRoom) canReconcile() bool {
	if m.isClosed() {
		return false
	}
	return true
}

func (m *RemoteRoom) reconcileParticipantRelayings() {
	var needsToReconcile []*participantRelaying
	m.lock.RLock()
	for _, relaying := range m.participantRelayings {
		if relaying.needsBeginRelay() || relaying.needsEndRelay() || relaying.needsCleanup() {
			needsToReconcile = append(needsToReconcile, relaying)
		}
	}
	m.lock.RUnlock()

	for _, r := range needsToReconcile {
		m.reconcileParticipantRelaying(r)
	}
}

func (m *RemoteRoom) reconcileParticipantRelaying(r *participantRelaying) {
	if !m.canReconcile() {
		return
	}

	if r.needsBeginRelay() {
		r.lock.Lock()
		r.state = RelayingStatus_STARTING
		pid := r.participantID
		r.lock.Unlock()

		// find the participant first
		participants := m.room.GetLocalParticipants()
		for _, p := range participants {
			if p.ID() == pid {
				r.participant = p
				break
			}
		}
		if r.participant == nil {
			m.logger.Errorw("participant not found", relay.ErrParticipantIsNotInRoom,
				"RoomName", m.room.Name(), "participantID", r.participantID)
			r.setDesired(false)
			return
		}

		rp, err := m.roomManager.CreateRelayParticipantToNode(r.participantID, m.remoteNodeID, m.room.Name())
		if err != nil {
			m.logger.Errorw("create relay participant failed", err, "ParticipantID", r.participantID,
				"remoteNodeID", m.remoteNodeID, "RoomName", m.room.Name())
			return
		}
		rp.AddOnClose(func() {
			r.lock.Lock()
			r.relayParticipant = nil
			r.state = RelayingStatus_IDLE
			r.lock.Unlock()
		})

		r.lock.Lock()
		r.relayParticipant = rp
		r.state = RelayingStatus_ACTIVE
		r.lock.Unlock()

		m.lock.Lock()
		m.relayParticipants[pid] = rp
		m.lock.Unlock()

		if r.isDesired() {
			go func() {
				// add to RelayManager
				err = r.participant.AddRelayParticipantToNode(m.remoteNodeID, rp)
				if err != nil {
					m.logger.Errorw("add relay participant to relay manager failed", err, "ParticipantID", r.participantID,
						"remoteNodeID", m.remoteNodeID, "RoomName", m.room.Name())

					// TODO: should add code to recover participant relay when failed
				}

				err := rp.Start()
				if err != nil {
					m.logger.Errorw("start relay participant failed", err, "ParticipantID", r.participantID,
						"remoteNodeID", m.remoteNodeID, "RoomName", m.room.Name())
				}
			}()
		}
		return
	}

	if r.needsEndRelay() {
		r.lock.Lock()
		participant := r.participant
		rp := r.relayParticipant
		r.state = RelayingStatus_ENDING
		r.lock.Unlock()

		go func() {
			_, err := participant.RemoveRelayParticipantFromNode(m.remoteNodeID)
			if err != nil {
				m.logger.Errorw("remove participant from relay manager failed!", err,
					"ParticipantID", r.participantID,
					"remoteNodeID", m.remoteNodeID, "RoomName", m.room.Name())
			}

			m.logger.Infow("closing relay participant", "destNodeID", rp.DestNodeID(),
				"Name", rp.Name())

			if !rp.IsClosed() {
				rp.AddOnClose(func() {
					r.lock.Lock()
					r.participant = nil
					r.relayParticipant = nil
					r.state = RelayingStatus_IDLE
					r.lock.Unlock()
				})

				rp.Close()
			} else {
				r.lock.Lock()
				r.participant = nil
				r.relayParticipant = nil
				r.state = RelayingStatus_IDLE
				r.lock.Unlock()
			}
		}()
		return
	}

	m.lock.Lock()
	if r.needsCleanup() {
		r.logger.Infow("cleanup removing participant relaying", "ParticipantID", r.participantID,
			"remoteNodeID", m.remoteNodeID, "RoomName", m.room.Name())
		delete(m.participantRelayings, r.participantID)
	}
	m.lock.Unlock()
}

func (m *RemoteRoom) GetRelayParticipants() []*relay.RelayParticipant {
	m.lock.RLock()
	defer m.lock.RUnlock()

	relayParticipants := make([]*relay.RelayParticipant, 0, len(m.participantRelayings))
	for _, relaying := range m.participantRelayings {
		rp := relaying.getRelayParticipant()
		if rp != nil {
			relayParticipants = append(relayParticipants, rp)
		}
	}
	return relayParticipants
}

// --------------------------------------------------------------------------------------

type RelayingStatus int32

const (
	RelayingStatus_IDLE     RelayingStatus = 0
	RelayingStatus_STARTING RelayingStatus = 1
	RelayingStatus_ACTIVE   RelayingStatus = 2
	RelayingStatus_ENDING   RelayingStatus = 3
)

type participantRelaying struct {
	participantID livekit.ParticipantID
	destNodeID    livekit.NodeID
	logger        logger.Logger

	lock            sync.RWMutex
	desired         bool
	changedNotifier types.ChangeNotifier
	removedNotifier types.ChangeNotifier
	numAttempts     atomic.Int32

	participant      types.LocalParticipant
	relayParticipant *relay.RelayParticipant
	state            RelayingStatus

	relayStartedAt atomic.Pointer[time.Time]
	relayAt        atomic.Pointer[time.Time]
}

func newParticipantRelaying(participantID livekit.ParticipantID, destNodeID livekit.NodeID, l logger.Logger) *participantRelaying {
	s := &participantRelaying{
		participantID: participantID,
		destNodeID:    destNodeID,
		logger:        l,
	}
	t := time.Now()
	s.relayAt.Store(&t)
	return s
}

func (r *participantRelaying) setDesired(desired bool) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	if desired {
		// as long as user explicitly set it to desired
		// we'll reset the timer so it has sufficient time to reconcile
		t := time.Now()
		r.relayStartedAt.Store(&t)
		r.relayAt.Store(&t)
	}

	if r.desired == desired {
		return false
	}
	r.desired = desired

	// when no longer desired, we no longer care about change notifications
	if desired {
		// reset attempts
		r.numAttempts.Store(0)
	} else {
		r.setChangedNotifierLocked(nil)
		r.setRemovedNotifierLocked(nil)
	}
	return true
}

func (r *participantRelaying) isDesired() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.desired
}

func (r *participantRelaying) setRelayParticipant(rp *relay.RelayParticipant) {
	r.lock.Lock()
	oldRelayParticipant := r.relayParticipant
	r.relayParticipant = rp
	r.lock.Unlock()

	if oldRelayParticipant != nil && !oldRelayParticipant.IsClosed() {
		r.logger.Infow("closing relay participant", "destNodeID", rp.DestNodeID(),
			"Name", rp.Name())

		go oldRelayParticipant.Close()
	}
}

func (r *participantRelaying) getRelayParticipant() *relay.RelayParticipant {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.relayParticipant
}

func (r *participantRelaying) setState(newState RelayingStatus) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.state = newState
}

func (r *participantRelaying) getState() RelayingStatus {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.state
}

func (r *participantRelaying) setChangedNotifier(notifier types.ChangeNotifier) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.setChangedNotifierLocked(notifier)
}

func (r *participantRelaying) setChangedNotifierLocked(notifier types.ChangeNotifier) bool {
	if r.changedNotifier == notifier {
		return false
	}

	existing := r.changedNotifier
	r.changedNotifier = notifier

	if existing != nil {
		go existing.RemoveObserver(string(r.participantID))
	}
	return true
}

func (r *participantRelaying) setRemovedNotifier(notifier types.ChangeNotifier) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.setRemovedNotifierLocked(notifier)
}

func (r *participantRelaying) setRemovedNotifierLocked(notifier types.ChangeNotifier) bool {
	if r.removedNotifier == notifier {
		return false
	}

	existing := r.removedNotifier
	r.removedNotifier = notifier

	if existing != nil {
		go existing.RemoveObserver(string(r.participantID))
	}
	return true
}

func (r *participantRelaying) needsBeginRelay() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return r.desired && r.relayParticipant == nil && r.state == RelayingStatus_IDLE
}

func (r *participantRelaying) needsEndRelay() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return !r.desired && r.relayParticipant != nil && r.state == RelayingStatus_ACTIVE
}

func (r *participantRelaying) needsCleanup() bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	return !r.desired && r.relayParticipant == nil // && r.state == RelayingStatus_IDLE
}
