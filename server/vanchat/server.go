// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"context"
	"fmt"
	"strings"
	"sync"

	// first-party libraries.
	cepb "github.com/vanus-labs/vanus/api/cloudevents"
)

const (
	sourcePrefix    = "vanchat:app:"
	sourcePrefixLen = len(sourcePrefix)
)

type server struct {
	books sync.Map // <string, book>
}

func (s *server) PostMessages(ctx context.Context, messages *cepb.CloudEventBatch) ([]int64, error) {
	appID, err := s.checkMessages(messages)
	if err != nil {
		return nil, err
	}

	var b *book
	if v, ok := s.books.Load(appID); ok {
		b = v.(*book)
	} else {
		return nil, fmt.Errorf("not found app: %s", appID)
	}

	p := NewPromise[[]int64]()
	b.PostMessages(ctx, messages, p.OnResult)
	return p.GetFuture().Wait()
}

func (s *server) checkMessages(messages *cepb.CloudEventBatch) (string, error) {
	if messages == nil || len(messages.Events) == 0 {
		return "", fmt.Errorf("empty messages")
	}

	// TODO: check messages
	source := messages.Events[0].GetSource()
	if !strings.HasPrefix(source, sourcePrefix) {
		return "", fmt.Errorf("invalid source: %s", source)
	}
	for _, m := range messages.Events[1:] {
		if m.GetSource() != source {
			return "", fmt.Errorf("invalid source: %s", m.GetSource())
		}
	}

	appID := source[sourcePrefixLen:]

	return appID, nil
}
