// SPDX-FileCopyrightText: 2022 Linkall Inc.
// SPDX-FileCopyrightText: 2015 The etcd Authors
//
// SPDX-License-Identifier: Apache-2.0

package raft

import pb "github.com/vanus-labs/vanus/pkg/raft/raftpb"

type recordKeeper struct {
	appendBuffer [][]pb.Entry
	sentMessages []pb.Message
}

func newTestKeeper() *recordKeeper {
	return &recordKeeper{}
}

// Make sure keeper implements Keeper interfaces.
var _ Keeper = (*recordKeeper)(nil)

func (rk *recordKeeper) OnStateChanged(st SoftState) {}

func (rk *recordKeeper) OnLeadCaughtUp() {}

func (rk *recordKeeper) OnNewSeal(seal uint64) {}

func (rk *recordKeeper) SetHardState(st pb.HardState) {}

func (rk *recordKeeper) CommitTo(index uint64) {}

func (rk *recordKeeper) TruncateAndAppend(ents []pb.Entry) {
	rk.appendBuffer = append(rk.appendBuffer, ents)
}

func (rk *recordKeeper) CompactTo(index uint64) {}

func (rk *recordKeeper) Apply(ents []pb.Entry) {}

func (rk *recordKeeper) Send(msg pb.Message) {
	rk.sentMessages = append(rk.sentMessages, msg)
}

func (rk *recordKeeper) readMessages(r *raft, s *MemoryStorage) []pb.Message {
	rk.advance(r, s)
	msgs := rk.sentMessages
	rk.sentMessages = nil
	return msgs
}

func (rk *recordKeeper) advance(r *raft, s *MemoryStorage) {
	for {
		appendBuffer := rk.appendBuffer
		rk.appendBuffer = nil

		if len(appendBuffer) == 0 {
			break
		}

		var msgsAfterAppend []pb.Message
		for _, ents := range appendBuffer {
			s.Append(ents)
			msgsAfterAppend = append(msgsAfterAppend, pb.Message{
				To:      r.id,
				Type:    pb.MsgLogStatus,
				LogTerm: ents[len(ents)-1].Term,
				Index:   ents[len(ents)-1].Index,
			})
		}
		rk.stepOrSend(r, msgsAfterAppend)
	}
}

// func (rk *recordKeeper) advanceMessagesAfterAppend(r *raft) {
// 	for {
// 		msgs := rk.takeMessagesAfterAppend()
// 		if len(msgs) == 0 {
// 			break
// 		}
// 		rk.stepOrSend(r, msgs)
// 	}
// }

// func (rk *recordKeeper) takeMessagesAfterAppend() []pb.Message {
// 	msgs := rk.msgsAfterAppend
// 	rk.msgsAfterAppend = nil
// 	return msgs
// }

func (rk *recordKeeper) stepOrSend(r *raft, msgs []pb.Message) error {
	for _, m := range msgs {
		if m.To == r.id {
			if err := r.Step(m); err != nil {
				return err
			}
		} else {
			rk.sentMessages = append(rk.sentMessages, m)
		}
	}
	return nil
}
