// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package relay

import "errors"

var (
	ErrHostOrPortNotProvided        = errors.New("host or port was not provided")
	ErrInvalidPort                  = errors.New("invalid port")
	ErrConnectionTimeout            = errors.New("could not connect after timeout")
	ErrTrackPublishTimeout          = errors.New("timed out publishing track")
	ErrCannotDetermineMime          = errors.New("cannot determine mimetype from file extension")
	ErrUnsupportedFileType          = errors.New("ReaderSampleProvider does not support this mime type")
	ErrUnsupportedSimulcastKind     = errors.New("simulcast is only supported for video")
	ErrInvalidSimulcastTrack        = errors.New("simulcast track was not initiated correctly")
	ErrCannotFindTrack              = errors.New("could not find the track")
	ErrInvalidParameter             = errors.New("invalid parameter")
	ErrCannotConnectSignal          = errors.New("could not establish signal connection")
	ErrCannotDialSignal             = errors.New("could not dial signal connection")
	ErrNoPeerConnection             = errors.New("peer connection not established")
	ErrAborted                      = errors.New("operation was aborted")
	ErrRelayParticipantAlreadyExist = errors.New("Relay participant already exist")
	ErrParticipantNotReadyForRelay  = errors.New("Participant not ready for relay")
	ErrParticipantRelaySignalBroken = errors.New("Participant's relay signal is broken")
	ErrNoRelayToRemoteNode          = errors.New("no relay to remote node")
	ErrParticipantIsNotPublisher    = errors.New("Participant is not publisher")
	ErrNoRemoteRoom                 = errors.New("no remote room found")
	ErrParticipantIsNotInRoom       = errors.New("participant in not in the room")
	ErrTrackCannotBeRelayed         = errors.New("track cannot be relayed")
	ErrRelayLimitExceeded           = errors.New("participant has exceeded its relay limit")
)
