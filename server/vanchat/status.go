// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

type result[T any] struct {
	value T
	err   error
}

type Future[T any] interface {
	Wait() (T, error)
}

type Promise[T any] interface {
	OnResult(value T, err error)
	GetFuture() Future[T]
}

type promise[T any] chan result[T]

func NewPromise[T any]() Promise[T] {
	return make(promise[T], 1)
}

func (p promise[T]) OnResult(value T, err error) {
	p <- result[T]{
		value: value,
		err:   err,
	}
}

func (p promise[T]) GetFuture() Future[T] {
	return p
}

func (p promise[T]) Wait() (T, error) {
	res := <-p
	return res.value, res.err
}
