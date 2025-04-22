// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import "github.com/vanus-labs/vanus/lib/executor"

type ReorderBuffer struct {
	seq     uint64
	next    uint64
	pending map[uint64]func()

	// callbackExecutor is a serial executor for callbacks.
	callbackExecutor executor.Executor
}

func (rb *ReorderBuffer) Prepare() uint64 {
	seq := rb.seq
	rb.seq++
	return seq
}

func (rb *ReorderBuffer) Commit(seq uint64, cb func()) {
	rb.callbackExecutor.Execute(func() {
		rb.commit(seq, cb)
	})
}

func (rb *ReorderBuffer) commit(seq uint64, cb func()) {
	if seq != rb.next {
		rb.pending[seq] = cb
		return
	}

	cb()

	seq++

	for cb, ok := rb.pending[seq]; ok; seq++ {
		cb()
	}

	rb.next = seq
}
