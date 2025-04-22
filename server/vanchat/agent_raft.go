// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"context"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/raft"
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"
)

// Run raft in the raftExecutor that conforms to the Actor model.

func (a *agent) step(msg *raftpb.Message) {
	a.raftExecutor.Execute(func() {
		_ = a.node.Step(*msg)
	})
}

func (a *agent) propose(opts raft.ProposeOptions) {
	a.raftExecutor.Execute(func() {
		a.node.Propose(opts)
	})
}

func (a *agent) reportStateStatus(_ context.Context, term, vote uint64) {
	a.raftExecutor.Execute(func() {
		_ = a.node.ReportStateStatus(term, vote)
	})
}

func (a *agent) reportLogStatus(_ context.Context, index, term uint64) {
	a.raftExecutor.Execute(func() {
		_ = a.node.ReportLogStatus(index, term)
	})
}

func (a *agent) reportApplyStatus(_ context.Context, index uint64) {
	a.raftExecutor.Execute(func() {
		_ = a.node.ReportApplyStatus(index)
	})
}

func (a *agent) reportCheckpointStatus(_ context.Context, index uint64) {
	a.raftExecutor.Execute(func() {
		_ = a.node.ReportCheckpointStatus(index)
	})
}

func (a *agent) reportUnreachable(id uint64) {
	a.raftExecutor.Execute(func() {
		a.node.ReportUnreachable(id)
	})
}

func (a *agent) tick() bool {
	return a.raftExecutor.Execute(func() {
		a.node.Tick()
	})
}

func (a *agent) bootstrap(peers []raft.Peer) error {
	ch := make(chan error, 1)
	ok := a.raftExecutor.Execute(func() {
		ch <- a.node.Bootstrap(peers)
	})
	if !ok {
		return raft.ErrStopped
	}
	// FIXME(james.yin): agent is stopped when bootstrap.
	return <-ch
}

func (a *agent) applyConfChange(index uint64, cc raftpb.ConfChangeI) {
	a.raftExecutor.Execute(func() {
		cs := a.node.ApplyConfChange(cc)
		a.setConfState(index, cs)
	})
}
