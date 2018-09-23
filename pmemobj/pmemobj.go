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

// Continuous blob of data allocable on persistent pool
type Blob interface {
	// Pointer to underlying data
	Ptr() unsafe.Pointer
	// Set value of underlying data based on provided unsafe pointer
	Set(p unsafe.Pointer)
	// Size of underlying data
	Size() uintptr
}

// Pool is a handler to pmemobj pool
type Pool struct {
	pop  *C.struct_pmemobjpool
	open bool
}

// Oid is a handler to object allocated on pmemobj pool
type Oid struct {
	oid C.struct_pmemoid
}

// Create creates new pmemobj pool
func Create(path, layout string, size uint64, mode uint16) (Pool, error) {
	pop, err := C.pmemobj_create(C.CString(path), C.CString(layout),
		C.size_t(size), C.uint(mode))
	return Pool{pop, pop != nil}, err
}

// Open opens existing pmemobj pool
func Open(path, layout string) (Pool, error) {
	pop, err := C.pmemobj_open(C.CString(path), C.CString(layout))
	return Pool{pop, pop != nil}, err
}

// Close closes the pmemobj pool
func (p *Pool) Close() {
	C.pmemobj_close(p.pop)
	p.open = false
}

// AllocBlob allocates new value on pmemobj pool
func (p *Pool) AllocBlob(value Blob, typeNum uintptr) (Oid, error) {
	var oid C.struct_pmemoid

	data := C.struct_data{C.size_t(value.Size()), value.Ptr()}
	cptr := C.malloc(C.size_t(unsafe.Sizeof(data)))
	if cptr == nil {
		return Oid{}, fmt.Errorf("C.malloc failed")
	}
	defer C.free(cptr)

	*(*C.struct_data)(cptr) = data

	i, err := C.pmemobj_alloc(p.pop, &oid, C.size_t(value.Size()),
		C.size_t(typeNum), C.pmemobj_constr(C.elem_constructor), cptr)
	if i != 0 {
		return Oid{}, err
	}

	return Oid{oid}, nil
}

// FirstBlob returns first object from the pool
func (p *Pool) FirstBlob(b Blob) (Oid, error) {
	oid := C.pmemobj_first(p.pop)
	if oid.off == 0 {
		return Oid{}, fmt.Errorf("oid is null")
	}
	b.Set(C.pmemobj_direct(oid))
	return Oid{oid}, nil
}

// NextBlob returns next element of provided Oid
func (o *Oid) NextBlob(b Blob) (Oid, error) {
	oid := C.pmemobj_next(o.oid)
	if oid.off == 0 {
		return Oid{}, fmt.Errorf("oid is null")
	}
	b.Set(C.pmemobj_direct(oid))
	return Oid{oid}, nil
}
