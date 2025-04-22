// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"context"

	// first-party libraries.
	"github.com/vanus-labs/vanus/lib/executor"
	"github.com/vanus-labs/vanus/pkg/raft"
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"

	// this project.
	"github.com/vanus-labs/vanus/server/store/raft/storage"
	"github.com/vanus-labs/vanus/server/store/raft/transport"
)

type App interface {
	Apply(index uint64, data []byte)
	OnLeaderChanged(leader uint64, becomeLeader bool)
	OnLeadCaughtUp()
	OnNewSeal(seal uint64)
}

type agent struct {
	app App

	node    *raft.RawNode
	storage *storage.Storage
	host    transport.Host

	leaderID uint64
	hint     map[uint64]string

	applyReorderBuffer ReorderBuffer

	raftExecutor      executor.ExecuteCloser
	commitExecutor    executor.ExecuteCloser
	persistExecutor   executor.ExecuteCloser
	applyExecutor     executor.ExecuteCloser
	transportExecutor executor.ExecuteCloser
}

func (a *agent) Propose(opts ...raft.ProposeOption) {
	po := raft.ProposeOptions{
		Data: make([]raft.ProposeData, 0, len(opts)),
	}
	for _, opt := range opts {
		opt(&po)
	}
	a.propose(po)
}

// CommitApply commits the apply operation. It supports out-of-order commits.
func (a *agent) CommitApply(index uint64) {
	a.applyReorderBuffer.Commit(index, func() {
		ctx := context.TODO()
		a.storage.SetApplied(ctx, index)
		a.reportApplyStatus(ctx, index)
	})
}

func (a *agent) ReportCheckpoint(index uint64) {
	a.reportCheckpointStatus(context.TODO(), index)
}

func (a *agent) RangeUnsealedEntries(f func(uint64, []byte) bool) {
	base, ents, err := a.node.UnsealedEntries()
	if err != nil {
		panic(err)
	}
	for i := range ents {
		ent := &ents[i]
		if ent.Type != raftpb.EntryNormal || ent.Data == nil {
			continue
		}
		if !f(base+uint64(i), ent.Data) {
			return
		}
	}
}

func (a *agent) changeConf(entry *raftpb.Entry) {
	var cci raftpb.ConfChangeI
	if entry.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
		cci = cc

		a.transportExecutor.Execute(func() {
			if cc.Type == raftpb.ConfChangeRemoveNode {
				delete(a.hint, cc.NodeID)
			} else {
				a.hint[cc.NodeID] = string(cc.Context)
			}
		})
	} else {
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(entry.Data); err != nil {
			panic(err)
		}
		cci = cc

		changes := cc.Changes
		a.transportExecutor.Execute(func() {
			// FIXME(james.yean): check it.
			for _, ccs := range changes {
				if ccs.Type == raftpb.ConfChangeRemoveNode {
					delete(a.hint, ccs.NodeID)
				} else {
					a.hint[ccs.NodeID] = string(cc.Context)
				}
			}
		})
	}

	a.applyConfChange(entry.Index, cci)
}

func (a *agent) setConfState(index uint64, cs *raftpb.ConfState) {
	a.commitExecutor.Execute(func() {
		a.storage.SetConfState(context.TODO(), *cs, func(err error) {
			if err != nil {
				panic(err)
			}
			a.applyExecutor.Execute(func() {
				a.CommitApply(index)
			})
		})
	})
}
