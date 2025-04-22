// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"context"
	"encoding/binary"
	"sync"

	// third-party libraries.
	"github.com/cloudevents/sdk-go/v2/event/datacodec"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	// first-party libraries.
	cepb "github.com/vanus-labs/vanus/api/cloudevents"
)

const (
	maxBufferSize = 100
)

type messageContent struct {
	Type string
	Text string `json:"text,omitempty" bson:"text,omitempty"`
}

type messageData struct {
	Content    []map[string]messageContent
	Extensions map[string]any
}

type mongoMessage struct {
	AppID           string
	AppSequence     int64
	SessionID       string
	SessionSequence int64
	MsgID           string
	Type            string
	AuthType        string
	AuthID          string
	Time            int64
	RecordedTime    int64
	Content         []map[string]messageContent
	Extensions      map[string]any
}

type messageBuffer struct {
	msgs []*cepb.CloudEvent
}

// Make sure messageBuffer implements Buffer[[]*ce.CloudEvent].
var _ Buffer[*cepb.CloudEvent] = (*messageBuffer)(nil)

func (mb *messageBuffer) Write(data []*cepb.CloudEvent) (int, error) {
	if free := maxBufferSize - len(mb.msgs); free < len(data) {
		mb.msgs = append(mb.msgs, data[:free]...)
		return free, nil
	}
	mb.msgs = append(mb.msgs, data...)
	return len(data), nil
}

func (mb *messageBuffer) Full() bool {
	return len(mb.msgs) >= maxBufferSize
}

func (mb *messageBuffer) Empty() bool {
	return len(mb.msgs) == 0
}

type messageBackend struct {
	coll *mongo.Collection
	pool sync.Pool
}

func NewMongoBackend(uri string, database string, collection string) (*messageBackend, error) {
	// Use the SetServerAPIOptions() method to set the Stable API version to 1
	serverAPI := options.ServerAPI(options.ServerAPIVersion1)
	opts := options.Client().ApplyURI(uri).SetServerAPIOptions(serverAPI)

	// Create a new client and connect to the server
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}

	coll := client.Database(database).Collection(collection)

	return &messageBackend{
		coll: coll,
		pool: sync.Pool{
			New: func() any {
				return &messageBuffer{
					msgs: make([]*cepb.CloudEvent, 0, maxBufferSize),
				}
			},
		},
	}, nil
}

// Make sure mongoBackend implements BufferedWriterOperations[*cepb.CloudEvent, *messageBuffer].
var _ BufferedWriterOperations[*cepb.CloudEvent, *messageBuffer] = (*messageBackend)(nil)

func (mb *messageBackend) AllocBuffer() *messageBuffer {
	return mb.pool.Get().(*messageBuffer)
}

func (mb *messageBackend) ReleaseBuffer(b *messageBuffer) {
	mb.pool.Put(b)
}

func (mb *messageBackend) WriteBuffer(b *messageBuffer, cb func(error)) {
	go func() {
		docs := make([]interface{}, 0, len(b.msgs))

		for _, msg := range b.msgs {
			bSeq, _ := binary.Varint(msg.Attributes["vcbsequence"].GetCeBytes())
			cSeq, _ := binary.Varint(msg.Attributes["vccsequence"].GetCeBytes())

			var data messageData
			datacodec.Decode(context.Background(),
				msg.Attributes["datacontenttype"].GetCeString(), msg.GetBinaryData(), &data)

			docs = append(docs, mongoMessage{
				AppID:           msg.Source[sourcePrefixLen:],
				AppSequence:     bSeq,
				SessionID:       msg.Attributes["subject"].GetCeString(),
				SessionSequence: cSeq,
				MsgID:           msg.Id,
				Type:            msg.Type,
				AuthType:        msg.Attributes["authtype"].GetCeString(),
				AuthID:          msg.Attributes["authid"].GetCeString(),
				Time:            msg.Attributes["time"].GetCeTimestamp().AsTime().UnixMilli(),
				RecordedTime:    msg.Attributes["recordedtime"].GetCeTimestamp().AsTime().UnixMilli(),
				Content:         data.Content,
				Extensions:      data.Extensions,
			})
		}

		// FIXME
		_, err := mb.coll.InsertMany(context.TODO(), docs)

		cb(err)
	}()
}
