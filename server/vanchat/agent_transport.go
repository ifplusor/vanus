// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"context"
	"time"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"

	// this project.
	"github.com/vanus-labs/vanus/server/store/raft/transport"
)

const (
	defaultSendTimeout = 80 * time.Millisecond
)

// Make sure agent implements transport.Receiver.
var _ transport.Receiver = (*agent)(nil)

func (a *agent) send(ctx context.Context, msg *raftpb.Message) {
	to := msg.To
	endpoint := a.hint[to]
	a.host.Send(ctx, msg, to, endpoint, func(err error) {
		if err != nil {
			log.Warn(ctx).Err(err).
				Uint64("to", to).
				Str("endpoint", endpoint).
				Msg("send message failed")
			a.reportUnreachable(msg.To)
		}
	})
}

// Receive implements transport.Receiver.
func (a *agent) Receive(_ context.Context, msg *raftpb.Message, from uint64, endpoint string) {
	a.transportExecutor.Execute(func() {
		if endpoint != "" && a.hint[from] != endpoint {
			a.hint[from] = endpoint
			// TODO
			// _ = a.e.RegisterNodeRecord(from, endpoint)
		}

		a.step(msg)
	})
}
