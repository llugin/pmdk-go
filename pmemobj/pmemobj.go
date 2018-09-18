package pmemobj

// #cgo LDFLAGS: -lpmemobj
// #include <libpmemobj.h>
import "C"

type Pool struct {
	pop *C.struct_pmemobjpool
}

func Create(path, layout string, size uint64, mode uint16) (Pool, error) {
	pop, err := C.pmemobj_create(C.CString(path), C.CString(layout), C.size_t(size), C.uint(mode))
	return Pool{pop}, err
}

func (p *Pool) Close() {
	C.pmemobj_close(p.pop)
}
