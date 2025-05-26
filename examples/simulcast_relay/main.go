package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/go-logr/stdr"
	"github.com/livekit/livekit-server/pkg/config"
	"github.com/livekit/livekit-server/pkg/routing"
	"github.com/livekit/livekit-server/pkg/rpc"
	"github.com/livekit/livekit-server/pkg/rtc/relay"
	"github.com/livekit/livekit-server/pkg/service"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils"
	"github.com/livekit/protocol/utils/guid"
	"github.com/livekit/psrpc"
	"github.com/nats-io/nats.go"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

const (
	defaultNatsHost = "localhost"     // 默认服务器地址
	defaultNatsPort = 4222            // 默认端口号
	defaultTimeout  = 5 * time.Second // 默认连接超时时间
)

func main() {
	var logger logger.Logger = logger.LogRLogger(stdr.New(log.Default()))

	// 定义命令行参数
	host := flag.String("nats-host", defaultNatsHost, "NATS server address (default to localhost)")
	port := flag.Int("port", defaultNatsPort, "NATS server port (default: 4222)")

	// 解析命令行参数
	flag.Parse()

	// 验证必需参数
	if *host == "" || *port == 0 {
		logger.Errorw("host and port must be set", relay.ErrHostOrPortNotProvided, "host", *host, "port", *port)
		flag.Usage()
		os.Exit(1)
	}

	// 检查端口范围
	if *port < 1 || *port > 65535 {
		logger.Errorw("port must be between 1 and 65535", relay.ErrInvalidPort, "port", *port)
		os.Exit(1)
	}

	// 构建完整的服务器地址
	natsAddr := fmt.Sprintf("%s:%d", *host, *port)

	nc, err := nats.Connect(natsAddr)
	if err != nil {
		logger.Errorw("connect nats-server failed", err, "nats address", natsAddr)
		os.Exit(1)
	}
	defer nc.Close()

	bus := psrpc.NewNatsMessageBus(nc)

	conf, err := config.NewConfig("", true, nil, nil)
	conf.Limit.NumTracks = 10
	node, err := routing.NewLocalNode(conf)

	localNode := node.NodeID()

	roomName := livekit.RoomName("user-room")
	localTracksMap := map[string]*relay.LocalTrack{}
	go runSimulcastVideos(&localTracksMap, func() {
		runRoomRelay(logger, localNode, roomName, bus, &localTracksMap)
	})

	select {}
}

func runRoomRelay(logger logger.Logger, localNode livekit.NodeID, roomName livekit.RoomName, bus psrpc.MessageBus, localTracksMap *map[string]*relay.LocalTrack) {
	client, err := rpc.NewTypedCloudRelayServiceClient(localNode, livekit.RoomName("room.*"), bus)
	if err != nil {
		logger.Errorw("create cloud relay service client failed", err, "LocalNode", localNode, "Room", roomName)
		os.Exit(1)
	}
	defer client.Close()

	signalRelayConfig := config.SignalRelayConfig{
		RetryTimeout:     7500 * time.Millisecond,
		MinRetryInterval: 500 * time.Millisecond,
		MaxRetryInterval: 4 * time.Second,
		StreamBufferSize: 1000,
		ConnectAttempts:  3,
	}
	relayClient, err := service.NewCloudRelayServiceSignalClientFromTypedClient(localNode, roomName, bus, signalRelayConfig, client)
	if err != nil {
		logger.Errorw("create cloud relay service signal client failed", err, "LocalNode", localNode, "Room", roomName)
		os.Exit(1)
	}

	logger.Infow("Broadcasting RoomOnline...", err, "Node", localNode, "Room", roomName)

	nodes, err := relayClient.RoomOnline(context.Background(), livekit.RoomName(roomName), false)
	if err != nil {
		logger.Errorw("send RoomOnline failed", err, "LocalNode", localNode, "Room", roomName)
		os.Exit(1)
	}

	if len(*nodes) == 0 {
		logger.Infow("No online nodes found", "LocalNode", localNode, "Room", roomName)
		os.Exit(1)
	}

	//log.Printf("RoomOnline echo %d room(s)\n", len(*nodes))

	for _, toNode := range *nodes {
		go func() {
			sid := livekit.ParticipantID(guid.New(utils.ParticipantPrefix))
			logger.Infow("Trying to create relay signal",
				"ToNode", string(toNode), "Room", roomName, "ParticipantSid", string(sid))

			clientInfo := &livekit.ClientInfo{
				Sdk:         livekit.ClientInfo_GO,
				Protocol:    6,
				Browser:     "",
				DeviceModel: "",
			}
			t := true
			f := false
			grants := &auth.ClaimGrants{
				Video: &auth.VideoGrant{
					RoomJoin:             true,
					Room:                 string(roomName),
					CanSubscribe:         &f,
					CanPublish:           &t,
					CanUpdateOwnMetadata: &t,
				},
			}

			pri := routing.ParticipantRelayInit{
				RoomName:       roomName,
				FromNode:       localNode,
				ToNode:         toNode,
				ID:             sid,
				Identity:       "user01",
				Name:           "user01",
				Client:         clientInfo,
				Grants:         grants,
				Region:         "",
				Reconnect:      false,
				AutoSubscribe:  false,
				AdaptiveStream: false,
				DisableICELite: true,
			}
			_, reqSink, resSource, err := relayClient.StartParticipantRelaySignal(context.Background(), roomName, toNode, pri)
			if err != nil {
				logger.Errorw("start relay signal client failed", err,
					"ToNode", string(toNode), "Room", roomName, "ParticipantSid", string(sid))
				os.Exit(1)
			}

			relaySignalClient := relay.NewRelaySignalClient(relay.RelaySignalClientParams{
				Logger:    logger,
				ReqSink:   reqSink,
				ResSource: resSource,
			})

			relayParticipant := relay.NewRelayParticipant(relay.RelayParticipantParams{
				Logger:       logger,
				RoomName:     roomName,
				NodeID:       toNode,
				SignalClient: relaySignalClient,
			})

			relaySignalClient.HandleJoin = func() {
				go func() {
					logger.Infow("handle join signal called!")

					trackID := guid.New("TR_")

					localTracks := make([]*relay.LocalTrack, 0)

					localTrack, err := relay.NewLocalTrack(logger, webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
						relay.WithSimulcast(trackID, &livekit.VideoLayer{
							Quality: livekit.VideoQuality_LOW,
							//Width:   320,
							//Height:  180,
						}))
					if err != nil {
						logger.Errorw("create VideoQuality_LOW local track failed", err, "Track", trackID)
						os.Exit(1)
					}
					localTracks = append(localTracks, localTrack)
					(*localTracksMap)["q"] = localTrack

					localTrack, err = relay.NewLocalTrack(logger, webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
						relay.WithSimulcast(trackID, &livekit.VideoLayer{
							Quality: livekit.VideoQuality_MEDIUM,
							//Width:   640,
							//Height:  360,
						}))
					if err != nil {
						logger.Errorw("create VideoQuality_MEDIUM local track failed", err, "Track", trackID)
						os.Exit(1)
					}
					localTracks = append(localTracks, localTrack)
					(*localTracksMap)["h"] = localTrack

					localTrack, err = relay.NewLocalTrack(logger, webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
						relay.WithSimulcast(trackID, &livekit.VideoLayer{
							Quality: livekit.VideoQuality_HIGH,
							//Width:   1280,
							//Height:  720,
						}))
					if err != nil {
						logger.Errorw("create VideoQuality_MEDIUM local track failed", err, "Track", trackID)
						os.Exit(1)
					}
					localTracks = append(localTracks, localTrack)
					(*localTracksMap)["f"] = localTrack

					_, err = relayParticipant.PublishSimulcastTrack(localTracks, &relay.TrackPublicationOptions{
						Name:   "Video",
						Source: livekit.TrackSource_CAMERA,
					})
					if err != nil {
						logger.Errorw("publish simulcast track failed", err, "Track", trackID)
						os.Exit(1)
					}

				}()
			}

			err = relayParticipant.Start()
			if err != nil {
				logger.Errorw("start relay participant failed", err, "Room", roomName)
				os.Exit(1)
			}

			defer func() {
				go relayParticipant.Close()
			}()

			select {}
		}()
	}

	// Block forever
	select {}
}

func runSimulcastVideos(localTracksMap *map[string]*relay.LocalTrack, onVideoReady func()) {
	// Everything below is the Pion WebRTC API! Thanks for using it ❤️.

	// Prepare the configuration
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	}

	// Create a new RTCPeerConnection
	peerConnection, err := webrtc.NewPeerConnection(config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if cErr := peerConnection.Close(); cErr != nil {
			fmt.Printf("cannot close peerConnection: %v\n", cErr)
		}
	}()

	outputTracks := map[string]*webrtc.TrackLocalStaticRTP{}

	// Create Track that we send video back to browser on
	outputTrack, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType: webrtc.MimeTypeVP8,
	}, "video_q", "pion_q")
	if err != nil {
		panic(err)
	}
	outputTracks["q"] = outputTrack

	outputTrack, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType: webrtc.MimeTypeVP8,
	}, "video_h", "pion_h")
	if err != nil {
		panic(err)
	}
	outputTracks["h"] = outputTrack

	outputTrack, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType: webrtc.MimeTypeVP8,
	}, "video_f", "pion_f")
	if err != nil {
		panic(err)
	}
	outputTracks["f"] = outputTrack

	if _, err = peerConnection.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		panic(err)
	}

	// Add this newly created track to the PeerConnection to send back video
	if _, err = peerConnection.AddTransceiverFromTrack(
		outputTracks["q"], webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly}); err != nil {
		panic(err)
	}
	if _, err = peerConnection.AddTransceiverFromTrack(
		outputTracks["h"],
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
	); err != nil {
		panic(err)
	}
	if _, err = peerConnection.AddTransceiverFromTrack(
		outputTracks["f"],
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
	); err != nil {
		panic(err)
	}

	// Read incoming RTCP packets
	// Before these packets are returned they are processed by interceptors. For things
	// like NACK this needs to be called.
	processRTCP := func(rtpSender *webrtc.RTPSender) {
		rtcpBuf := make([]byte, 1500)
		for {
			if _, _, rtcpErr := rtpSender.Read(rtcpBuf); rtcpErr != nil {
				return
			}
		}
	}
	for _, rtpSender := range peerConnection.GetSenders() {
		go processRTCP(rtpSender)
	}

	// Wait for the offer to be pasted
	offer := webrtc.SessionDescription{}
	decode(readUntilNewline(), &offer)

	if err = peerConnection.SetRemoteDescription(offer); err != nil {
		panic(err)
	}

	// Set a handler for when a new remote track starts
	peerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) { //nolint: revive

		// Start reading from all the streams and sending them to the related output track
		rid := track.RID()
		fmt.Printf("Track rid:%q, ssrc:%d has started\n", rid, track.SSRC())

		if track.Kind() == webrtc.RTPCodecTypeVideo {
			go func() {
				ticker := time.NewTicker(3 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					fmt.Printf("Sending pli for stream with rid: %q, ssrc: %d\n", track.RID(), track.SSRC())
					if writeErr := peerConnection.WriteRTCP(
						[]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}},
					); writeErr != nil {
						fmt.Println(writeErr)
					}
				}
			}()
		}

		for {
			// Read RTP packets being sent to Pion
			packet, _, readErr := track.ReadRTP()
			if readErr != nil {
				panic(readErr)
			}

			if writeErr := outputTracks[rid].WriteRTP(packet); writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
				panic(writeErr)
			}

			if localTracksMap != nil && len(*localTracksMap) > 0 && (*localTracksMap)[rid] != nil {
				if writeErr := (*localTracksMap)[rid].WriteRTP(packet, nil); writeErr != nil && !errors.Is(writeErr, io.ErrClosedPipe) {
					panic(writeErr)
				}
			}
		}
	})

	// Set the handler for Peer connection state
	// This will notify you when the peer has connected/disconnected
	peerConnection.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		fmt.Printf("Peer Connection State has changed: %s\n", state.String())

		if state == webrtc.PeerConnectionStateFailed {
			// Wait until PeerConnection has had no network activity for 30 seconds or another failure.
			// It may be reconnected using an ICE Restart.
			// Use webrtc.PeerConnectionStateDisconnected if you are interested in detecting faster timeout.
			// Note that the PeerConnection may come back from PeerConnectionStateDisconnected.
			fmt.Println("Peer Connection has gone to failed exiting")
			os.Exit(0)
		}

		if state == webrtc.PeerConnectionStateClosed {
			// PeerConnection was explicitly closed. This usually happens from a DTLS CloseNotify
			fmt.Println("Peer Connection has gone to closed exiting")
			os.Exit(0)
		}

		if state == webrtc.PeerConnectionStateConnected {
			if onVideoReady != nil {
				go onVideoReady()
			}
		}
	})

	// Create an answer
	answer, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		panic(err)
	}

	// Create channel that is blocked until ICE Gathering is complete
	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)

	// Sets the LocalDescription, and starts our UDP listeners
	err = peerConnection.SetLocalDescription(answer)
	if err != nil {
		panic(err)
	}

	// Block until ICE Gathering is complete, disabling trickle ICE
	// we do this because we only can exchange one signaling message
	// in a production application you should exchange ICE Candidates via OnICECandidate
	<-gatherComplete

	// Output the answer in base64 so we can paste it in browser
	fmt.Println(encode(peerConnection.LocalDescription()))

	// Block forever
	select {}
}

// Read from stdin until we get a newline.
func readUntilNewline() (in string) {
	var err error

	r := bufio.NewReader(os.Stdin)
	for {
		in, err = r.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			panic(err)
		}

		if in = strings.TrimSpace(in); len(in) > 0 {
			break
		}
	}

	fmt.Println("")

	return
}

// JSON encode + base64 a SessionDescription.
func encode(obj *webrtc.SessionDescription) string {
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}

	return base64.StdEncoding.EncodeToString(b)
}

// Decode a base64 and unmarshal JSON into a SessionDescription.
func decode(in string, obj *webrtc.SessionDescription) {
	b, err := base64.StdEncoding.DecodeString(in)
	if err != nil {
		panic(err)
	}

	if err = json.Unmarshal(b, obj); err != nil {
		panic(err)
	}
}
