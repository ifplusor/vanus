// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	pb "github.com/vanus-labs/vanus/pkg/raft/raftpb"
)

type Keeper interface {
	StateListener
	StateKeeper
	LogKeeper
	AppKeeper
	NetKeeper
}

type StateListener interface {
	OnStateChanged(st SoftState)
	OnLeadCaughtUp()
	OnNewSeal(seal uint64)
}

type StateKeeper interface {
	SetHardState(st pb.HardState)
	CommitTo(index uint64)
}

type LogKeeper interface {
	TruncateAndAppend(ents []pb.Entry)
	CompactTo(index uint64)
}

type AppKeeper interface {
	Apply(ents []pb.Entry)
}

type NetKeeper interface {
	Send(msg pb.Message)
}
