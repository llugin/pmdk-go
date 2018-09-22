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

// #cgo LDFLAGS: -lpmemobj
// #include <libpmemobj.h>
// #include <stdlib.h>
//
// struct data {
// size_t size;
// void* val;
// };
//
// int elem_constructor(PMEMobjpool *pop, void *ptr, void *arg) {
//	struct data* d = (struct data*)arg;
//  memcpy(ptr, d->val, d->size);
//	pmemobj_persist(pop, ptr, d->size);
//  return 0;
//  }
//
import "C"

import (
	"fmt"
	"unsafe"
)

// Pool is a handler to pmemobj pool
type Pool struct {
	pop *C.struct_pmemobjpool
}

// Oid is a handler to object allocated on pmemobj pool
type Oid struct {
	oid C.struct_pmemoid
}

// Create creates new pmemobj pool
func Create(path, layout string, size uint64, mode uint16) (Pool, error) {
	pop, err := C.pmemobj_create(C.CString(path), C.CString(layout), C.size_t(size), C.uint(mode))
	return Pool{pop}, err
}

// Open opens existing pmemobj pool
func Open(path, layout string) (Pool, error) {
	pop, err := C.pmemobj_open(C.CString(path), C.CString(layout))
	return Pool{pop}, err
}

// Close closes the pmemobj pool
func (p *Pool) Close() {
	C.pmemobj_close(p.pop)
}

// Alloc allocates new value on pmemobj pool
func (p *Pool) Alloc(value int) (Oid, error) {
	var oid C.struct_pmemoid

	data := C.struct_data{C.size_t(unsafe.Sizeof(value)), unsafe.Pointer(&value)}
	cptr := C.malloc(C.size_t(unsafe.Sizeof(data)))
	if cptr == nil {
		return Oid{}, fmt.Errorf("C.malloc fail")
	}
	defer C.free(cptr)

	*(*C.struct_data)(cptr) = data

	i, err := C.pmemobj_alloc(p.pop, &oid, C.size_t(data.size), 1, C.pmemobj_constr(C.elem_constructor), cptr)
	if i != 0 {
		return Oid{}, err
	}

	return Oid{oid}, nil
}

// First returns first object from the pool
func (p *Pool) First() (int, error) {
	oid := C.pmemobj_first(p.pop)
	if oid.off == 0 {
		return 0, fmt.Errorf("oid is null")
	}
	ptr := C.pmemobj_direct(oid)
	return *(*int)(ptr), nil
}
