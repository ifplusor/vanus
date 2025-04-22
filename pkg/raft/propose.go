// Copyright 2022 Linkall Inc.
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

package raft

import (
	// standard libraries.
	"context"

	// this project.
	pb "github.com/vanus-labs/vanus/pkg/raft/raftpb"
)

type ProposeCallback = func(error)

type ProposeData struct {
	Data         []byte
	Callback     ProposeCallback
	NoWaitCommit bool
}

type ProposeDataOption func(pd *ProposeData)

func Data(data []byte) ProposeDataOption {
	return func(pd *ProposeData) {
		pd.Data = data
	}
}

func Callback(cb ProposeCallback) ProposeDataOption {
	return func(pd *ProposeData) {
		pd.Callback = cb
	}
}

func NoWaitCommit() ProposeDataOption {
	return func(pd *ProposeData) {
		pd.NoWaitCommit = true
	}
}

type ProposeOptions struct {
	Data              []ProposeData
	BeforeProposeHook func([]pb.Entry)
	AfterProposeHook  func([]pb.Entry, error)
}

type ProposeOption func(po *ProposeOptions)

func WithData(opts ...ProposeDataOption) ProposeOption {
	return func(po *ProposeOptions) {
		data := ProposeData{}
		for _, opt := range opts {
			opt(&data)
		}
		po.Data = append(po.Data, data)
	}
}

func WithBeforeProposeHook(hook func([]pb.Entry)) ProposeOption {
	return func(po *ProposeOptions) {
		po.BeforeProposeHook = hook
	}
}

func WithAfterProposeHook(hook func([]pb.Entry, error)) ProposeOption {
	return func(po *ProposeOptions) {
		po.AfterProposeHook = hook
	}
}

func Propose(ctx context.Context, n Node, opts ...ProposeOption) {
	po := ProposeOptions{
		Data: make([]ProposeData, 0, len(opts)),
	}
	for _, opt := range opts {
		opt(&po)
	}
	n.Propose(ctx, po.Data...)
}

type proposeFuture chan error

func newProposeFuture() proposeFuture {
	return make(proposeFuture, 1)
}

func (pf proposeFuture) onProposed(err error) {
	if err != nil {
		pf <- err
	}
	close(pf)
}

func (pf proposeFuture) wait() error {
	return <-pf
}

func Propose0(ctx context.Context, n Node, data []byte) error {
	future := newProposeFuture()
	n.Propose(ctx, ProposeData{Data: data, Callback: future.onProposed})
	return future.wait()
}

func Propose1(ctx context.Context, n Node, data []byte) error {
	future := newProposeFuture()
	n.Propose(ctx, ProposeData{Data: data, Callback: future.onProposed, NoWaitCommit: true})
	return future.wait()
}

func Propose2(ctx context.Context, n Node, data []byte) {
	n.Propose(ctx, ProposeData{Data: data})
}
