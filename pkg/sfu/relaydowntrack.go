package sfu

import (
	"github.com/livekit/livekit-server/pkg/sfu/buffer"
	"github.com/livekit/livekit-server/pkg/sfu/mime"
	"github.com/livekit/livekit-server/pkg/sfu/rtpstats"
	"github.com/livekit/livekit-server/pkg/sfu/utils"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"go.uber.org/atomic"
	"math"
	"math/rand"
	"sync"
	"time"
)

// RelayTrackSender defines an interface send media to remote node
type RelayTrackSender interface {
	//UpTrackLayersChange()
	//UpTrackBitrateAvailabilityChange()
	//UpTrackMaxPublishedLayerChange(maxPublishedLayer int32)
	//UpTrackMaxTemporalLayerSeenChange(maxTemporalLayerSeen int32)
	//UpTrackBitrateReport(availableLayers []int32, bitrates Bitrates)
	WriteRTP(p *buffer.ExtPacket, layer int32) error
	Close()
	IsClosed() bool
	// ID is the globally unique identifier for this Track.
	ID() string
	RelayDestNodeID() livekit.NodeID
	//HandleRTCPSenderReportData(
	//	payloadType webrtc.PayloadType,
	//	isSVC bool,
	//	layer int32,
	//	publisherSRData *livekit.RTCPSenderReportState,
	//) error
	//Resync()
	SetReceiver(TrackReceiver)
}

type RelayDownTrackParams struct {
	Codecs          []webrtc.RTPCodecParameters
	Source          livekit.TrackSource
	Receiver        TrackReceiver
	RelayDestNodeID livekit.NodeID
	StreamID        string
	//MaxTrack                       int
	PlayoutDelayLimit *livekit.PlayoutDelay
	//Pacer                          pacer.Pacer
	Logger  logger.Logger
	Trailer []byte
	//RTCPWriter                     func([]rtcp.Packet) error
	//DisableSenderReportPassThrough bool
	//SupportsCodecChange            bool

	IsSimulcast       bool
	LocalTrackSenders []LocalTrackSender
}

// RelayDownTrack implements TrackLocal, is the track used to write packets
// to remote SFU node, the track handle the packets for simple, simulcast
// and SVC Publisher.
// A RelayDownTrack has the following lifecycle
// - new
// - bound / unbound
// - closed
// once closed, a RelayDownTrack cannot be re-used.
type RelayDownTrack struct {
	params            RelayDownTrackParams
	id                livekit.TrackID
	kind              webrtc.RTPCodecType
	ssrc              uint32
	ssrcRTX           uint32
	payloadType       atomic.Uint32
	payloadTypeRTX    atomic.Uint32
	sequencer         *sequencer
	rtxSequenceNumber atomic.Uint64

	receiverLock sync.RWMutex
	receiver     TrackReceiver

	forwarder *Forwarder

	upstreamCodecs            []webrtc.RTPCodecParameters
	codec                     webrtc.RTPCodecCapability
	clockRate                 uint32
	negotiatedCodecParameters []webrtc.RTPCodecParameters

	// payload types for red codec only
	isRED             bool
	upstreamPrimaryPT uint8
	primaryPT         uint8

	absSendTimeExtID          int
	transportWideExtID        int
	dependencyDescriptorExtID int
	playoutDelayExtID         int
	absCaptureTimeExtID       int
	//transceiver               atomic.Pointer[webrtc.RTPTransceiver]
	//writeStream               webrtc.TrackLocalWriter
	//rtcpReader    *buffer.RTCPReader
	//rtcpReaderRTX *buffer.RTCPReader

	//listenerLock            sync.RWMutex
	//receiverReportListeners []ReceiverReportListener

	bindLock sync.Mutex
	//bindState           atomic.Value
	//onBinding           func(error)
	//bindOnReceiverReady func()

	isClosed             atomic.Bool
	connected            atomic.Bool
	bindAndConnectedOnce atomic.Bool
	//writable             atomic.Bool
	writeStopped    atomic.Bool
	isReceiverReady bool

	rtpStats                   *rtpstats.RTPStatsSender
	deltaStatsSenderSnapshotId uint32

	rtpStatsRTX                   *rtpstats.RTPStatsSender
	deltaStatsRTXSenderSnapshotId uint32

	//totalRepeatedNACKs atomic.Uint32
	//
	//blankFramesGeneration atomic.Uint32
	//
	//connectionStats *connectionquality.ConnectionStats

	isNACKThrottled atomic.Bool

	//activePaddingOnMuteUpTrack atomic.Bool
	//
	//streamAllocatorLock     sync.RWMutex
	//streamAllocatorListener DownTrackStreamAllocatorListener
	//probeClusterId          atomic.Uint32

	playoutDelay *PlayoutDelayController

	//pacer pacer.Pacer

	//maxLayerNotifierChMu     sync.RWMutex
	//maxLayerNotifierCh       chan string
	//maxLayerNotifierChClosed bool

	keyFrameRequesterChMu     sync.RWMutex
	keyFrameRequesterCh       chan struct{}
	keyFrameRequesterChClosed bool

	cbMu sync.RWMutex
	//onStatsUpdate               func(dt *DownTrack, stat *livekit.AnalyticsStat)
	//onMaxSubscribedLayerChanged func(dt *DownTrack, layer int32)
	onRttUpdate    func(dt *DownTrack, rtt uint32)
	onCloseHandler func(isExpectedToResume bool)
	//onCodecNegotiated func(webrtc.RTPCodecCapability)

	createdAt int64
}

// NewRelayDownTrack returns a DownTrack.
func NewRelayDownTrack(params RelayDownTrackParams) (*RelayDownTrack, error) {
	codecs := params.Codecs
	mimeType := mime.NormalizeMimeType(codecs[0].MimeType)
	var kind webrtc.RTPCodecType
	switch {
	case mime.IsMimeTypeAudio(mimeType):
		kind = webrtc.RTPCodecTypeAudio
	case mime.IsMimeTypeVideo(mimeType):
		kind = webrtc.RTPCodecTypeVideo
	default:
		kind = webrtc.RTPCodecType(0)
	}

	d := &RelayDownTrack{
		params:         params,
		id:             params.Receiver.TrackID(),
		upstreamCodecs: codecs,
		kind:           kind,
		codec:          codecs[0].RTPCodecCapability,
		clockRate:      codecs[0].ClockRate,
		//pacer:               params.Pacer,
		//maxLayerNotifierCh:  make(chan string, 1),
		keyFrameRequesterCh: make(chan struct{}, 1),
		createdAt:           time.Now().UnixNano(),
		receiver:            params.Receiver,
	}
	//d.bindState.Store(bindStateUnbound)
	d.params.Logger = params.Logger.WithValues(
		"RelayDestNodeID", d.RelayDestNodeID(),
	)

	var mdCacheSize, mdCacheSizeRTX int
	if d.kind == webrtc.RTPCodecTypeVideo {
		mdCacheSize, mdCacheSizeRTX = 8192, 8192
	} else {
		mdCacheSize, mdCacheSizeRTX = 8192, 1024
	}
	d.rtpStats = rtpstats.NewRTPStatsSender(rtpstats.RTPStatsParams{
		ClockRate: d.codec.ClockRate,
		Logger: d.params.Logger.WithValues(
			"stream", "primary",
		),
	}, mdCacheSize)
	d.deltaStatsSenderSnapshotId = d.rtpStats.NewSenderSnapshotId()

	d.rtpStatsRTX = rtpstats.NewRTPStatsSender(rtpstats.RTPStatsParams{
		ClockRate: d.codec.ClockRate,
		IsRTX:     true,
		Logger: d.params.Logger.WithValues(
			"stream", "rtx",
		),
	}, mdCacheSizeRTX)
	d.deltaStatsRTXSenderSnapshotId = d.rtpStatsRTX.NewSenderSnapshotId()

	d.forwarder = NewForwarder(
		d.kind,
		d.params.Logger,
		false,
		d.rtpStats,
	)

	//d.connectionStats = connectionquality.NewConnectionStats(connectionquality.ConnectionStatsParams{
	//	SenderProvider: d,
	//	Logger:         d.params.Logger.WithValues("direction", "down"),
	//})
	//d.connectionStats.OnStatsUpdate(func(_cs *connectionquality.ConnectionStats, stat *livekit.AnalyticsStat) {
	//	if onStatsUpdate := d.getOnStatsUpdate(); onStatsUpdate != nil {
	//		onStatsUpdate(d, stat)
	//	}
	//})

	if d.kind == webrtc.RTPCodecTypeVideo {
		if delay := params.PlayoutDelayLimit; delay.GetEnabled() {
			var err error
			d.playoutDelay, err = NewPlayoutDelayController(delay.GetMin(), delay.GetMax(), params.Logger, d.rtpStats)
			if err != nil {
				return nil, err
			}
		}
		//go d.maxLayerNotifierWorker()
		go d.keyFrameRequester()
	}

	//d.params.Receiver.AddOnReady(d.handleReceiverReady)
	d.rtxSequenceNumber.Store(uint64(rand.Intn(1<<14)) + uint64(1<<15)) // a random number in third quartile of sequence number space

	d.params.Logger.Debugw("RelayDownTrack created", "upstreamCodecs", d.upstreamCodecs)

	return d, nil
}

func (d *RelayDownTrack) handleUpstreamCodecChange(mimeType string) {
	d.bindLock.Lock()
	if mime.IsMimeTypeStringEqual(d.codec.MimeType, mimeType) {
		d.bindLock.Unlock()
		return
	}

	//if !d.params.SupportsCodecChange {
	//	d.bindLock.Unlock()
	//	d.params.Logger.Infow("client doesn't support codec change, renegotiate new codec")
	//	go d.Close()
	//	return
	//}

	oldPT, oldRtxPT, oldCodec := d.payloadType.Load(), d.payloadTypeRTX.Load(), d.codec

	var codec webrtc.RTPCodecParameters
	for _, c := range d.upstreamCodecs {
		if !mime.IsMimeTypeStringEqual(c.MimeType, mimeType) {
			continue
		}

		matchCodec, err := utils.CodecParametersFuzzySearch(c, d.negotiatedCodecParameters)
		if err == nil {
			codec = matchCodec
			break
		}
	}

	if codec.MimeType == "" {
		// codec not found, should not happen since the upstream codec should only fall back to higher compatibility (vp8)
		d.params.Logger.Errorw(
			"can't find matched codec for new upstream payload type", nil,
			"upstreamCodecs", d.upstreamCodecs,
			"remoteParameters", d.negotiatedCodecParameters,
			"mime", mimeType,
		)
		d.bindLock.Unlock()
		return
	}

	d.payloadType.Store(uint32(codec.PayloadType))
	d.payloadTypeRTX.Store(uint32(utils.FindRTXPayloadType(codec.PayloadType, d.negotiatedCodecParameters)))
	d.codec = codec.RTPCodecCapability
	//newMimeType := d.mimeTypeLocked()
	//isFECEnabled := strings.Contains(strings.ToLower(d.codec.SDPFmtpLine), "fec")
	d.bindLock.Unlock()

	d.params.Logger.Infow(
		"upstream codec changed",
		"oldPT", oldPT, "newPT", d.payloadType.Load(),
		"oldRTXPT", oldRtxPT, "newRTXPT", d.payloadTypeRTX.Load(),
		"oldCodec", oldCodec, "newCodec", codec.RTPCodecCapability,
	)

	d.forwarder.Restart()
	d.forwarder.DetermineCodec(codec.RTPCodecCapability, d.Receiver().HeaderExtensions())
	//d.connectionStats.UpdateCodec(newMimeType, isFECEnabled)
}

// ID is the unique identifier for this Track. This should be unique for the
// stream, but doesn't have to globally unique. A common example would be 'audio' or 'video'
// and StreamID would be 'desktop' or 'webcam'
func (d *RelayDownTrack) ID() string { return string(d.id) }

// Codec returns current track codec capability
func (d *RelayDownTrack) Codec() webrtc.RTPCodecCapability {
	d.bindLock.Lock()
	defer d.bindLock.Unlock()
	return d.codec
}

func (d *RelayDownTrack) Mime() mime.MimeType {
	d.bindLock.Lock()
	defer d.bindLock.Unlock()
	return d.mimeTypeLocked()
}

func (d *RelayDownTrack) mimeTypeLocked() mime.MimeType {
	return mime.NormalizeMimeType(d.codec.MimeType)
}

// StreamID is the group this track belongs too. This must be unique
func (d *RelayDownTrack) StreamID() string { return d.params.StreamID }

func (d *RelayDownTrack) RelayDestNodeID() livekit.NodeID {
	// add `createdAt` to ensure repeated subscriptions from same subscriber to same publisher does not collide
	//return livekit.NodeID(fmt.Sprintf("%s:%d", d.params.RelayDestNodeID, d.createdAt))
	return d.params.RelayDestNodeID
}

func (d *RelayDownTrack) Receiver() TrackReceiver {
	d.receiverLock.RLock()
	defer d.receiverLock.RUnlock()
	return d.receiver
}

func (d *RelayDownTrack) SetReceiver(r TrackReceiver) {
	d.params.Logger.Infow("relay down track set receiver", "codec", r.Codec())
	d.bindLock.Lock()
	if d.IsClosed() {
		d.bindLock.Unlock()
		return
	}

	d.receiverLock.Lock()
	old := d.receiver
	d.receiver = r
	d.receiverLock.Unlock()

	old.DeleteRelayDownTrack(d.RelayDestNodeID())
	if err := r.AddRelayDownTrack(d); err != nil {
		d.params.Logger.Warnw("failed to add relay down track to receiver", err)
	}
	d.bindLock.Unlock()

	//r.AddOnReady(d.handleReceiverReady)
	d.handleUpstreamCodecChange(r.Codec().MimeType)
	//if sal := d.getStreamAllocatorListener(); sal != nil {
	//	sal.OnSubscribedLayerChanged(d, d.forwarder.MaxLayer())
	//}
}

// Kind controls if this TrackLocal is audio or video
func (d *RelayDownTrack) Kind() webrtc.RTPCodecType {
	return d.kind
}

func (d *RelayDownTrack) postKeyFrameRequestEvent() {
	if d.kind != webrtc.RTPCodecTypeVideo {
		return
	}

	d.keyFrameRequesterChMu.RLock()
	if !d.keyFrameRequesterChClosed {
		select {
		case d.keyFrameRequesterCh <- struct{}{}:
		default:
		}
	}
	d.keyFrameRequesterChMu.RUnlock()
}

func (d *RelayDownTrack) keyFrameRequester() {
	getInterval := func() time.Duration {
		interval := 2 * d.rtpStats.GetRtt()
		//if interval < keyFrameIntervalMin {
		//	interval = keyFrameIntervalMin
		//}
		//if interval > keyFrameIntervalMax {
		interval = keyFrameIntervalMax
		//}
		return time.Duration(interval) * time.Millisecond
	}

	timer := time.NewTimer(math.MaxInt64)
	timer.Stop()

	defer timer.Stop()

	for !d.IsClosed() {
		timer.Reset(getInterval())

		select {
		case _, more := <-d.keyFrameRequesterCh:
			if !more {
				return
			}
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}

		if d.isWritable() {
			trackInfo := d.Receiver().TrackInfo()
			d.params.Logger.Debugw("sending PLI for layer lock")
			for layer, _ := range trackInfo.Layers {
				d.Receiver().SendPLI(int32(layer), false)
				d.rtpStats.UpdateLayerLockPliAndTime(1)
			}
		}

		//locked, layer := d.forwarder.CheckSync()
		//if !locked && layer != buffer.InvalidLayerSpatial && d.isWritable() {
		//	d.params.Logger.Infow("sending PLI for layer lock", "layer", layer)
		//	d.Receiver().SendPLI(layer, false)
		//	d.rtpStats.UpdateLayerLockPliAndTime(1)
		//} else {
		//	d.params.Logger.Infow("sending PLI for layer lock failed", "layer", layer)
		//}
	}
}

func (d *RelayDownTrack) isWritable() bool {
	if len(d.params.LocalTrackSenders) > 0 {
		return d.params.LocalTrackSenders[0].IsBound()
	}
	return false
}

// WriteRTP writes an RTP Packet to the DownTrack
func (d *RelayDownTrack) WriteRTP(extPkt *buffer.ExtPacket, layer int32) error {
	if !d.isWritable() {
		//d.params.Logger.Infow("Not writable!")
		return nil
	}

	senders := d.params.LocalTrackSenders
	if d.params.IsSimulcast {
		if layer < int32(len(senders)) {
			//d.params.Logger.Infow("Simulcast WriteRTP", "layer", layer,
			//	"PayloadType", extPkt.Packet.PayloadType, "SSRC", extPkt.Packet.SSRC,
			//	"PayloadLength", len(extPkt.Packet.Payload))
			sender := senders[layer]
			err := sender.WriteRTP(extPkt.Packet, nil)
			if err != nil {
				d.params.Logger.Errorw("Simulcast WriteRTP failed", err, "layer", layer,
					"PayloadType", extPkt.Packet.PayloadType, "SSRC", extPkt.Packet.SSRC,
					"PayloadLength", len(extPkt.Packet.Payload))
			}
		}
	} else {
		//d.params.Logger.Infow("Non-Simulcast WriteRTP", "layer", layer,
		//	"PayloadType", extPkt.Packet.PayloadType, "SSRC", extPkt.Packet.SSRC,
		//	"PayloadLength", len(extPkt.Packet.Payload))
		sender := senders[0]
		sender.WriteRTP(extPkt.Packet, nil)
	}

	//tp, err := d.forwarder.GetTranslationParams(extPkt, layer)
	//if tp.shouldDrop {
	//	if err != nil {
	//		d.params.Logger.Errorw("could not get translation params", err)
	//	}
	//	return err
	//}
	//
	//poolEntity := PacketFactory.Get().(*[]byte)
	//payload := *poolEntity
	//copy(payload, tp.codecBytes)
	//n := copy(payload[len(tp.codecBytes):], extPkt.Packet.Payload[tp.incomingHeaderSize:])
	//if n != len(extPkt.Packet.Payload[tp.incomingHeaderSize:]) {
	//	d.params.Logger.Errorw("payload overflow", nil, "want", len(extPkt.Packet.Payload[tp.incomingHeaderSize:]), "have", n)
	//	PacketFactory.Put(poolEntity)
	//	return ErrPayloadOverflow
	//}
	//payload = payload[:len(tp.codecBytes)+n]
	//
	//// translate RTP header
	//hdr := &rtp.Header{
	//	Version:        extPkt.Packet.Version,
	//	Padding:        extPkt.Packet.Padding,
	//	PayloadType:    d.getTranslatedPayloadType(extPkt.Packet.PayloadType),
	//	SequenceNumber: uint16(tp.rtp.extSequenceNumber),
	//	Timestamp:      uint32(tp.rtp.extTimestamp),
	//	SSRC:           d.ssrc,
	//}
	//if tp.marker {
	//	hdr.Marker = tp.marker
	//}
	//
	//// add extensions
	//if d.dependencyDescriptorExtID != 0 && tp.ddBytes != nil {
	//	hdr.SetExtension(uint8(d.dependencyDescriptorExtID), tp.ddBytes)
	//}
	//if d.playoutDelayExtID != 0 && d.playoutDelay != nil {
	//	if val := d.playoutDelay.GetDelayExtension(hdr.SequenceNumber); val != nil {
	//		hdr.SetExtension(uint8(d.playoutDelayExtID), val)
	//
	//		// NOTE: play out delay extension is not cached in sequencer,
	//		// i. e. they will not be added to retransmitted packet.
	//		// But, it is okay as the extension is added till a RTCP Receiver Report for
	//		// the corresponding sequence number is received.
	//		// The extreme case is all packets containing the play out delay are lost and
	//		// all of them retransmitted and an RTCP Receiver Report received for those
	//		// retransmitted sequence numbers. But, that is highly improbable, if not impossible.
	//	}
	//}
	//var actBytes []byte
	//if extPkt.AbsCaptureTimeExt != nil && d.absCaptureTimeExtID != 0 {
	//	// normalize capture time to SFU clock.
	//	// NOTE: even if there is estimated offset populated, just re-map the
	//	// absolute capture time stamp as it should be the same RTCP sender report
	//	// clock domain of publisher. SFU is normalising sender reports of publisher
	//	// to SFU clock before sending to subscribers. So, capture time should be
	//	// normalized to the same clock. Clear out any offset.
	//	_, _, refSenderReport := d.forwarder.GetSenderReportParams()
	//	if refSenderReport != nil {
	//		actExtCopy := *extPkt.AbsCaptureTimeExt
	//		if err = actExtCopy.Rewrite(
	//			rtpstats.RTCPSenderReportPropagationDelay(
	//				refSenderReport,
	//				true, //!d.params.DisableSenderReportPassThrough,
	//			),
	//		); err == nil {
	//			actBytes, err = actExtCopy.Marshal()
	//			if err == nil {
	//				hdr.SetExtension(uint8(d.absCaptureTimeExtID), actBytes)
	//			}
	//		}
	//	}
	//}
	//d.addDummyExtensions(hdr)
	//
	//if d.sequencer != nil {
	//	d.sequencer.push(
	//		extPkt.Arrival,
	//		extPkt.ExtSequenceNumber,
	//		tp.rtp.extSequenceNumber,
	//		tp.rtp.extTimestamp,
	//		hdr.Marker,
	//		int8(layer),
	//		payload[:len(tp.codecBytes)],
	//		tp.incomingHeaderSize,
	//		tp.ddBytes,
	//		actBytes,
	//	)
	//}
	//
	//headerSize := hdr.MarshalSize()
	//d.rtpStats.Update(
	//	extPkt.Arrival,
	//	tp.rtp.extSequenceNumber,
	//	tp.rtp.extTimestamp,
	//	hdr.Marker,
	//	headerSize,
	//	len(payload),
	//	0,
	//	extPkt.IsOutOfOrder,
	//)
	////d.pacer.Enqueue(&pacer.Packet{
	////	Header:             hdr,
	////	HeaderSize:         headerSize,
	////	Payload:            payload,
	////	ProbeClusterId:     ccutils.ProbeClusterId(d.probeClusterId.Load()),
	////	AbsSendTimeExtID:   uint8(d.absSendTimeExtID),
	////	TransportWideExtID: uint8(d.transportWideExtID),
	////	WriteStream:        d.writeStream,
	////	Pool:               PacketFactory,
	////	PoolEntity:         poolEntity,
	////})
	//
	//if extPkt.KeyFrame {
	//	d.isNACKThrottled.Store(false)
	//	d.rtpStats.UpdateKeyFrame(1)
	//	d.params.Logger.Debugw(
	//		"forwarded key frame",
	//		"layer", layer,
	//		"rtpsn", tp.rtp.extSequenceNumber,
	//		"rtpts", tp.rtp.extTimestamp,
	//	)
	//}
	//
	////if tp.isSwitching {
	////	d.postMaxLayerNotifierEvent("switching")
	////}
	////
	////if tp.isResuming {
	////	if sal := d.getStreamAllocatorListener(); sal != nil {
	////		sal.OnResume(d)
	////	}
	////}
	return nil
}

func (d *RelayDownTrack) addDummyExtensions(hdr *rtp.Header) {
	// add dummy extensions (actual ones will be filed by pacer) to get header size
	if d.absSendTimeExtID != 0 {
		hdr.SetExtension(uint8(d.absSendTimeExtID), dummyAbsSendTimeExt)
	}
	if d.transportWideExtID != 0 {
		hdr.SetExtension(uint8(d.transportWideExtID), dummyTransportCCExt)
	}
}

func (d *RelayDownTrack) getTranslatedPayloadType(src uint8) uint8 {
	// send primary codec to subscriber if the publisher send primary codec to us when red is negotiated,
	// this will happen when the payload is too large to encode into red payload (exceeds mtu).
	if d.isRED && src == d.upstreamPrimaryPT && d.primaryPT != 0 {
		return d.primaryPT
	}
	return uint8(d.payloadType.Load())
}

func (d *RelayDownTrack) IsClosed() bool {
	return d.isClosed.Load()
}

func (d *RelayDownTrack) Close() {
	d.CloseWithFlush(true)
}

// CloseWithFlush - flush used to indicate whether send blank frame to flush
// decoder of client.
//  1. When transceiver is reused by other participant's video track,
//     set flush=true to avoid previous video shows before new stream is displayed.
//  2. in case of session migration, participant migrate from other node, video track should
//     be resumed with same participant, set flush=false since we don't need to flush decoder.
func (d *RelayDownTrack) CloseWithFlush(flush bool) {
	if d.isClosed.Swap(true) {
		// already closed
		return
	}

	d.bindLock.Lock()
	d.params.Logger.Debugw("close down track", "flushBlankFrame", flush)
	//if d.bindState.Load() == bindStateBound {
	//	d.forwarder.Mute(true, true)
	//
	//	// write blank frames after disabling so that other frames do not interfere.
	//	// Idea here is to send blank key frames to flush the decoder buffer at the remote end.
	//	// Otherwise, with transceiver re-use last frame from previous stream is held in the
	//	// display buffer and there could be a brief moment where the previous stream is displayed.
	//	if flush {
	//		doneFlushing := d.writeBlankFrameRTP(RTPBlankFramesCloseSeconds, d.blankFramesGeneration.Inc())
	//
	//		// wait a limited time to flush
	//		timer := time.NewTimer(flushTimeout)
	//		defer timer.Stop()
	//
	//		select {
	//		case <-doneFlushing:
	//		case <-timer.C:
	//			d.blankFramesGeneration.Inc() // in case flush is still running
	//		}
	//	}
	//
	//	d.params.Logger.Debugw("closing sender", "kind", d.kind)
	//}

	//d.setBindStateLocked(bindStateUnbound)
	d.Receiver().DeleteRelayDownTrack(d.RelayDestNodeID())

	//if d.rtcpReader != nil && flush {
	//	d.params.Logger.Debugw("relay down track close rtcp reader")
	//	d.rtcpReader.Close()
	//	d.rtcpReader.OnPacket(nil)
	//}
	//if d.rtcpReaderRTX != nil && flush {
	//	d.params.Logger.Debugw("relay down track close rtcp rtx reader")
	//	d.rtcpReaderRTX.Close()
	//	d.rtcpReaderRTX.OnPacket(nil)
	//}
	mime := d.codec.MimeType
	d.bindLock.Unlock()

	//d.connectionStats.Close()

	d.rtpStats.Stop()
	d.rtpStatsRTX.Stop()
	d.params.Logger.Debugw("rtp stats",
		"direction", "downstream",
		"mime", mime,
		"ssrc", d.ssrc,
		"stats", d.rtpStats,
		"statsRTX", d.rtpStatsRTX,
	)

	//d.maxLayerNotifierChMu.Lock()
	//d.maxLayerNotifierChClosed = true
	//close(d.maxLayerNotifierCh)
	//d.maxLayerNotifierChMu.Unlock()

	d.keyFrameRequesterChMu.Lock()
	d.keyFrameRequesterChClosed = true
	close(d.keyFrameRequesterCh)
	d.keyFrameRequesterChMu.Unlock()

	//if onCloseHandler := d.getOnCloseHandler(); onCloseHandler != nil {
	//	onCloseHandler(!flush)
	//}
}

// OnCloseHandler method to be called on remote tracked removed
func (d *RelayDownTrack) OnCloseHandler(fn func(isExpectedToResume bool)) {
	d.cbMu.Lock()
	defer d.cbMu.Unlock()

	d.onCloseHandler = fn
}
