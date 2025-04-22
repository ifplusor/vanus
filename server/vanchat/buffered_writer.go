// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	// standard libraries.
	"sync"

	// this project.
	"github.com/vanus-labs/vanus/server/store/io/stream"
)

type AsyncWriter[T any] interface {
	Write(data []T, cb func())
}

type Buffer[T any] interface {
	Write(data []T) (int, error)
	Full() bool
	Empty() bool
}

type BufferedWriterOperations[T any, B Buffer[T]] interface {
	AllocBuffer() B
	ReleaseBuffer(b B)
	WriteBuffer(b B, cb func(error))
	// TODO: timer
	// StartTimer()
}

type comparableBuffer[T any] interface {
	comparable
	Buffer[T]
}

type BufferedWriter[T any, B comparableBuffer[T]] struct {
	mu sync.Mutex

	buf B
	// dirty is a flag to indicate whether has data in the buffer to be flushed.
	dirty   bool
	waiting []func()

	rob ReorderBuffer

	timer stream.PendingID

	op BufferedWriterOperations[T, B]
}

func (bw *BufferedWriter[T, B]) Write(data []T, cb func()) {
	var null, last B

	bw.mu.Lock()
	defer bw.mu.Unlock()

	for off := 0; off < len(data); {
		if bw.buf == null {
			bw.buf = bw.op.AllocBuffer()
		}

		n, err := bw.buf.Write(data[off:])
		if err != nil {
			panic(err)
		}

		if n == 0 {
			continue
		}

		if bw.buf.Full() {
			if bw.dirty {
				bw.dirty = false
				bw.cancelFlushTimer()
			}

			if last != null {
				bw.flushBuffer(last, bw.waiting)
				bw.waiting = nil
			}

			bw.buf, last = null, bw.buf
		}

		off += n
	}

	// got EOF

	empty := bw.buf == null || bw.buf.Empty()

	if last != null {
		var waiting []func()
		if empty {
			waiting, bw.waiting = append(bw.waiting, cb), nil
		} else {
			waiting, bw.waiting = bw.waiting, []func(){cb}
		}
		bw.flushBuffer(last, waiting)
	} else {
		bw.waiting = append(bw.waiting, cb)
	}

	if (!empty || last == null) && !bw.dirty {
		bw.dirty = true
		bw.startFlushTimer()
	}
}

func (bw *BufferedWriter[T, B]) OnTimeout(pid stream.PendingID) {
	var null B

	bw.mu.Lock()
	defer bw.mu.Unlock()

	if !bw.dirty {
		return
	}

	bw.flushBuffer(bw.buf, bw.waiting)

	bw.buf, bw.waiting = null, nil
	bw.dirty = false
}

func (bw *BufferedWriter[T, B]) flushBuffer(b B, cbs []func()) {
	seq := bw.rob.Prepare()
	bw.op.WriteBuffer(b, func(err error) {
		bw.op.ReleaseBuffer(b)
		bw.rob.Commit(seq, func() {
			if err != nil {
				panic(err)
			}
			// TODO
			for _, cb := range cbs {
				cb()
			}
		})
	})
}

func (bw *BufferedWriter[T, B]) startFlushTimer() {
	if bw.timer != nil {
		return
	}
	// TODO
}

func (bw *BufferedWriter[T, B]) cancelFlushTimer() {
	if bw.timer == nil {
		return
	}
	// TODO
	bw.timer = nil
}
