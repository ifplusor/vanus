// SPDX-FileCopyrightText: 2025 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vanchat

import (
	"fmt"
	"testing"
)

type buffer struct {
	full  bool
	empty bool
}

func (b *buffer) Write(data []byte) (int, error) {
	return 0, nil
}

func (b *buffer) Full() bool {
	return b.full
}

func (b *buffer) Empty() bool {
	return b.empty
}

func isNil[T any, B comparableBuffer[T]](b B) bool {
	var null B
	return b == null
}

func TestBuffer(t *testing.T) {
	var b *buffer
	if isNil(b) == true {
		fmt.Println("Ok.")
	}

	b = &buffer{}

	a := b.Full

	fmt.Printf("%v\n", a())

	b.full = true

	fmt.Printf("%v\n", a())

	if isNil(b) == false {
		fmt.Println("Ok.")
	}
}
