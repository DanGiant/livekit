package sfu

import (
	"sync"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/utils"
)

type RelayDownTrackSpreader struct {
	params DownTrackSpreaderParams

	relayDownTrackMu      sync.RWMutex
	relayDownTracks       map[livekit.NodeID]RelayTrackSender
	relayDownTracksShadow []RelayTrackSender
}

func NewRelayDownTrackSpreader(params DownTrackSpreaderParams) *RelayDownTrackSpreader {
	d := &RelayDownTrackSpreader{
		params:          params,
		relayDownTracks: make(map[livekit.NodeID]RelayTrackSender),
	}

	return d
}

func (d *RelayDownTrackSpreader) GetRelayDownTracks() []RelayTrackSender {
	d.relayDownTrackMu.RLock()
	defer d.relayDownTrackMu.RUnlock()
	return d.relayDownTracksShadow
}

func (d *RelayDownTrackSpreader) ResetAndGetRelayDownTracks() []RelayTrackSender {
	d.relayDownTrackMu.Lock()
	defer d.relayDownTrackMu.Unlock()

	relayDownTracks := d.relayDownTracksShadow

	d.relayDownTracks = make(map[livekit.NodeID]RelayTrackSender)
	d.relayDownTracksShadow = nil

	return relayDownTracks
}

func (d *RelayDownTrackSpreader) Store(ts RelayTrackSender) {
	d.relayDownTrackMu.Lock()
	defer d.relayDownTrackMu.Unlock()

	d.relayDownTracks[ts.RelayDestNodeID()] = ts
	d.shadowRelayDownTracks()
}

func (d *RelayDownTrackSpreader) Free(destNodeID livekit.NodeID) {
	d.relayDownTrackMu.Lock()
	defer d.relayDownTrackMu.Unlock()

	delete(d.relayDownTracks, destNodeID)
	d.shadowRelayDownTracks()
}

func (d *RelayDownTrackSpreader) HasRelayDownTrack(destNodeID livekit.NodeID) bool {
	d.relayDownTrackMu.RLock()
	defer d.relayDownTrackMu.RUnlock()

	_, ok := d.relayDownTracks[destNodeID]
	return ok
}

func (d *RelayDownTrackSpreader) Broadcast(writer func(RelayTrackSender)) int {
	relayDownTracks := d.GetRelayDownTracks()
	if len(relayDownTracks) == 0 {
		return 0
	}

	threshold := uint64(d.params.Threshold)
	if threshold == 0 {
		threshold = 1000000
	}

	// 100µs is enough to amortize the overhead and provide sufficient load balancing.
	// WriteRTP takes about 50µs on average, so we write to 2 relay tracks per loop.
	step := uint64(2)
	utils.ParallelExec(relayDownTracks, threshold, step, writer)
	return len(relayDownTracks)
}

func (d *RelayDownTrackSpreader) RelayDownTrackCount() int {
	d.relayDownTrackMu.RLock()
	defer d.relayDownTrackMu.RUnlock()
	return len(d.relayDownTracksShadow)
}

func (d *RelayDownTrackSpreader) shadowRelayDownTracks() {
	d.relayDownTracksShadow = make([]RelayTrackSender, 0, len(d.relayDownTracks))
	for _, dt := range d.relayDownTracks {
		d.relayDownTracksShadow = append(d.relayDownTracksShadow, dt)
	}
}
