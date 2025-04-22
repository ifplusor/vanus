// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"context"
	"errors"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/raft"
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"
	"github.com/vanus-labs/vanus/server/store/raft/storage"
)

type agentKeeper struct {
	*agent
}

// Make sure agent implements raft.Keeper.
var _ raft.Keeper = (*agentKeeper)(nil)

// All of these methods are called by raft in raftExecutor.

func (ak *agentKeeper) OnStateChanged(st raft.SoftState) {
	// TODO(james.yean): dispatch to another goroutine.
	ak.app.OnLeaderChanged(st.Lead, st.RaftState == raft.StateLeader)
}

func (ak *agentKeeper) OnLeadCaughtUp() {
	// TODO(james.yean): dispatch to another goroutine.
	ak.app.OnLeadCaughtUp()
}

func (ak *agentKeeper) OnNewSeal(seal uint64) {
	ak.app.OnNewSeal(seal)
}

func (ak *agentKeeper) SetHardState(st raftpb.HardState) {
	ak.commitExecutor.Execute(func() {
		ak.storage.SetHardState(context.TODO(), st, func(err error) {
			if err != nil {
				panic(err)
			}
			ak.reportStateStatus(context.TODO(), st.Term, st.Vote)
		})
	})
}

func (ak *agentKeeper) CommitTo(index uint64) {
	ak.commitExecutor.Execute(func() {
		ak.storage.SetCommit(context.TODO(), index)
	})
}

func (ak *agentKeeper) TruncateAndAppend(ents []raftpb.Entry) {
	ak.persistExecutor.Execute(func() {
		ak.storage.Append(context.TODO(), ents, func(re storage.AppendResult, err error) {
			if err != nil {
				if errors.Is(err, storage.ErrCompacted) || errors.Is(err, storage.ErrTruncated) {
					// FIXME(james.yin): report to raft?
					return
				}
				panic(err)
			}

			// Report entries has been persisted.
			ak.reportLogStatus(context.TODO(), re.Index, re.Term)
		})
	})
}

func (ak *agentKeeper) CompactTo(index uint64) {
	ak.persistExecutor.Execute(func() {
		_ = ak.storage.Compact(context.TODO(), index)
	})
}

func (ak *agentKeeper) Apply(ents []raftpb.Entry) {
	ak.applyExecutor.Execute(func() {
		for i := 0; i < len(ents); i++ {
			entry := &ents[i]

			if ak.applyReorderBuffer.Prepare() != entry.Index {
				panic("apply entries out of order")
			}

			// FIXME:(james.yean): check it.
			if entry.Type != raftpb.EntryNormal {
				ak.changeConf(entry)
				continue
			}

			if entry.Data == nil {
				ak.CommitApply(entry.Index)
				continue
			}

			ak.app.Apply(entry.Index, entry.Data)
		}
	})
}

func (ak *agentKeeper) Send(msg raftpb.Message) {
	ak.transportExecutor.Execute(func() {
		ctx, cancel := context.WithTimeout(context.TODO(), defaultSendTimeout)
		defer cancel()
		ak.send(ctx, &msg)
	})
}
