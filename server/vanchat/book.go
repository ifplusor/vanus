// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	// third-party libraries.
	"github.com/huandu/skiplist"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	// first-party libraries.
	cepb "github.com/vanus-labs/vanus/api/cloudevents"
	"github.com/vanus-labs/vanus/lib/container/conque/unbounded"
	"github.com/vanus-labs/vanus/lib/executor"
	"github.com/vanus-labs/vanus/pkg/raft"
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"
)

type AppendCallback = func(seqs []int64, err error)

type postRequest struct {
	ctx      context.Context
	messages *cepb.CloudEventBatch
	cb       AppendCallback
}

type conversation struct {
	id string

	mu       sync.RWMutex
	version  int64
	offset   int64
	messages []*cepb.CloudEvent // TODO: use skiplist

	metaMu sync.RWMutex
	loaded bool

	// seq is the next sequence number to be assigned.
	seq int64

	appendExecutor executor.ExecuteCloser
	pendingRequest unbounded.Queue[postRequest]
}

type bookStateType int

const (
	bookStateFollower bookStateType = iota
	bookStateLeaderInitializing
	bookStateLeader
)

type book struct {
	id            string
	conversations sync.Map // <string, *conversation>

	mu               sync.RWMutex
	conversationList *skiplist.SkipList

	metaMu sync.RWMutex
	state  bookStateType

	// seq is the next sequence number to be assigned.
	seq int64

	raftAgent     *agent
	backendWriter AsyncWriter[*cepb.CloudEvent]
}

func newBook() *book {
	return &book{
		conversationList: skiplist.New(skiplist.Int64Desc),
	}
}

func (b *book) PostMessages(ctx context.Context, messages *cepb.CloudEventBatch, cb AppendCallback) {
	b.metaMu.RLock()
	defer b.metaMu.RUnlock()
	if b.state != bookStateLeader {
		// TODO
		cb(nil, fmt.Errorf("not leader"))
		return
	}

	conversationID := messages.Events[0].GetAttributes()["subject"].GetCeString()

	var c *conversation
	if v, ok := b.conversations.Load(conversationID); ok {
		c = v.(*conversation)
	} else {
		c = &conversation{
			id: conversationID,
		}
		// maybe concurrently post to the same conversation,
		if v, loaded := b.conversations.LoadOrStore(conversationID, c); loaded {
			c = v.(*conversation)
		} else {
			go b.loadConversationMetadata(c)
		}
	}

	c.metaMu.RLock()
	defer c.metaMu.RUnlock()

	if !c.loaded {
		c.pendingRequest.Push(postRequest{
			ctx:      ctx,
			messages: messages,
			cb:       cb,
		})
		return
	}

	c.appendExecutor.Execute(func() {
		b.appendToConversation(c, messages, cb)
	})
}

func (b *book) loadConversationMetadata(c *conversation) {
	// TODO: 查询Conversation的元数据
	var seq int64
	c.offset = seq
	c.seq = seq

	// resume pending requests
	for {
		pr, empty, ok := c.pendingRequest.UniquePop()
		if ok {
			select {
			case <-pr.ctx.Done():
				// FIXME
				pr.cb(nil, pr.ctx.Err())
			default:
				c.appendExecutor.Execute(func() {
					b.appendToConversation(c, pr.messages, pr.cb)
				})
			}
		}
		if empty {
			break
		}
	}

	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	c.loaded = true
}

func (b *book) appendToConversation(c *conversation, messages *cepb.CloudEventBatch, cb AppendCallback) {
	now := time.Now()
	for _, msg := range messages.Events {
		msg.Attributes["recordedtime"] = &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeTimestamp{
				CeTimestamp: timestamppb.New(now),
			},
		}
	}

	data, err := proto.MarshalOptions{}.MarshalAppend(make([]byte, 16), messages)
	if err != nil {
		cb(nil, err)
		return
	}

	seqs := make([]int64, len(messages.Events))
	b.raftAgent.Propose(
		raft.WithBeforeProposeHook(func(entries []raftpb.Entry) {
			binary.LittleEndian.PutUint64(entries[0].Data[0:8], uint64(b.seq))
			binary.LittleEndian.PutUint64(entries[0].Data[8:16], uint64(c.seq))
			for i := range messages.Events {
				seqs[i] = c.seq + int64(i)
			}
		}), raft.WithAfterProposeHook(func(entries []raftpb.Entry, err error) {
			if err != nil {
				return
			}
			n := int64(len(entries))
			b.seq += n
			c.seq += n
		}), raft.WithData(raft.Data(data), raft.Callback(func(err error) {
			if err != nil {
				cb(nil, err)
				return
			}
			// FIXME(james.yean): wake up pending read request after apply.
			cb(seqs, nil)
		})),
	)
}

// Make sure book implements App.
var _ App = (*book)(nil)

func (b *book) Apply(index uint64, data []byte) {
	msgs := b.genericApply(index, data)

	b.metaMu.RLock()
	state := b.state
	b.metaMu.RUnlock()

	if state == bookStateLeader {
		b.backendWriter.Write(msgs, func() {
			// FIXME: build conversation list
			b.raftAgent.ReportCheckpoint(index)
		})
	}
}

func (b *book) genericApply(index uint64, data []byte) []*cepb.CloudEvent {
	bOff, cOff, msgs := decodeMessageBatch(data)
	newMsgs := b.applyMessageBatch(bOff, cOff, msgs)
	b.raftAgent.CommitApply(index)
	return newMsgs
}

func decodeMessageBatch(data []byte) (int64, int64, []*cepb.CloudEvent) {
	bOff := int64(binary.LittleEndian.Uint64(data[0:8]))
	cOff := int64(binary.LittleEndian.Uint64(data[8:16]))

	var msgs cepb.CloudEventBatch
	if err := proto.Unmarshal(data[16:], &msgs); err != nil {
		panic("failed to unmarshal messages: " + err.Error())
	}

	for i, msg := range msgs.Events {
		// sequence in book
		msg.Attributes["vcbsequence"] = &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeBytes{
				CeBytes: binary.AppendVarint(nil, bOff+int64(i)),
			},
		}
		// sequence in conversation
		msg.Attributes["vccsequence"] = &cepb.CloudEvent_CloudEventAttributeValue{
			Attr: &cepb.CloudEvent_CloudEventAttributeValue_CeBytes{
				CeBytes: binary.AppendVarint(nil, cOff+int64(i)),
			},
		}
	}

	return bOff, cOff, msgs.Events
}

func (b *book) applyMessageBatch(bOff, cOff int64, msgs []*cepb.CloudEvent) []*cepb.CloudEvent {
	conversationID := msgs[0].GetAttributes()["subject"].GetCeString()

	var c *conversation
	if v, ok := b.conversations.Load(conversationID); ok {
		c = v.(*conversation)
	} else {
		// Receive messages of a unloaded conversation, so must not be the leader, and no apply-post conflict.
		c = &conversation{
			id:     conversationID,
			offset: cOff,
			// loaded: true,
		}
		b.conversations.Store(conversationID, c)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	addition := msgs
	if next := c.offset + int64(len(c.messages)); next != cOff {
		// TODO: logging

		if cOff > next {
			panic("messages not continuous")
		}

		overlap := int(next - cOff)
		if len(msgs) <= overlap {
			return nil
		}
		addition = msgs[overlap:]
	}
	c.messages = append(c.messages, addition...)

	// TODO(james.yean): Update conversation list.
	version := bOff + int64(len(msgs)) - 1
	if c.version != 0 {
		b.conversationList.Remove(c.version)
	}
	b.conversationList.Set(version, c)
	c.version = version

	return addition
}

func (b *book) OnLeaderChanged(leader uint64, becomeLeader bool) {
	// TODO: record leader ID

	if !becomeLeader && b.state == bookStateFollower {
		return
	}

	b.metaMu.Lock()
	defer b.metaMu.Unlock()

	if becomeLeader {
		b.state = bookStateLeaderInitializing
	} else {
		b.state = bookStateFollower
	}
}

func (b *book) OnLeadCaughtUp() {
	// Recover the sequence numbers for all conversations.
	b.conversations.Range(func(k any, v any) bool {
		c := v.(*conversation)
		c.seq = c.offset + int64(len(c.messages))
		c.loaded = true
		return true
	})

	b.raftAgent.RangeUnsealedEntries(func(index uint64, data []byte) bool {
		_, _, msgs := decodeMessageBatch(data)
		b.backendWriter.Write(msgs, func() {
			b.raftAgent.ReportCheckpoint(index)
		})
		return true
	})
	// FIXME(james.yean): reset b.seq

	b.metaMu.Lock()
	defer b.metaMu.Unlock()

	b.state = bookStateLeader
}

func (b *book) OnNewSeal(seal uint64) {
	b.raftAgent.ReportCheckpoint(seal)
}
