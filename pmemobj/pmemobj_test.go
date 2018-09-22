/*
 * Copyright 2018, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package pmemobj

import (
	"os"
	"testing"
)

const poolPath = "pool"

func tearDown(p Pool) {
	p.Close()
	os.Remove(poolPath)
}

func TestCreate(t *testing.T) {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		t.Error(err)
	}
	defer tearDown(pool)
}

func TestOpen(t *testing.T) {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		t.Error(err)
	}
	defer tearDown(pool)

	pool.Close()
	pool, err = Open(poolPath, "")
	if err != nil {
		t.Error(err)
	}
}

func TestReopen(t *testing.T) {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		t.Error(err)
	}
	defer tearDown(pool)

	for i := 0; i < 3; i++ {
		pool.Close()
		pool, err = Open(poolPath, "")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestAlloc(t *testing.T) {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		t.Error(err)
	}
	defer tearDown(pool)

	var oid Oid
	oid, err = pool.Alloc(888)

	if err != nil {
		t.Errorf("%v. Oid: %v", err, oid)
	}
}

func TestRead(t *testing.T) {
	var err error
	var pool Pool
	pool, err = Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		t.Error(err)
	}
	defer tearDown(pool)

	var oid Oid
	allocated := 888
	oid, err = pool.Alloc(allocated)

	if err != nil {
		t.Errorf("%v. Oid: %v", err, oid)
	}

	var val int
	val, err = pool.First()
	if err != nil {
		t.Error(err)
	}
	if val != allocated {
		t.Errorf("Val is equal %d but should be %d", val, allocated)
	}
}

func TestReadEmpty(t *testing.T) {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		t.Error(err)
	}
	defer tearDown(pool)

	_, err = pool.First()
	if err == nil {
		t.Error("No error despite reading from empty pool")
	}
}

func BenchmarkCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pool, err := Create(poolPath, "", 10*1024*1024, 0666)

		b.StopTimer()
		if err != nil {
			b.Error(err)
		}
		tearDown(pool)
		b.StartTimer()
	}
}

func BenchmarkOpen(b *testing.B) {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		b.Error(err)
	}
	pool.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool, err = Open(poolPath, "")

		b.StopTimer()

		if err != nil {
			b.Error(err)
			os.Remove(poolPath)
		}
		pool.Close()

		b.StartTimer()
	}

	os.Remove(poolPath)
}
