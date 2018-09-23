package pmemobj

import (
	"os"
	"testing"
	"unsafe"
)

const poolPath = "pool"

type blobInt struct {
	data int
}

func (b blobInt) Ptr() unsafe.Pointer {
	return unsafe.Pointer(&b.data)
}
func (b blobInt) Size() uintptr {
	return unsafe.Sizeof(b.data)
}
func (b *blobInt) Set(p unsafe.Pointer) {
	b.data = *(*int)(p)
}

type myStruct struct {
	intData    int
	stringData string
}

type blobStruct struct {
	my myStruct
}

func (b blobStruct) Ptr() unsafe.Pointer {
	return unsafe.Pointer(&b.my)
}
func (b blobStruct) Size() uintptr {
	return unsafe.Sizeof(b.my)
}
func (b *blobStruct) Set(p unsafe.Pointer) {
	b.my = *(*myStruct)(p)
}

func helperCreateT(t *testing.T) Pool {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func helperCreateB(b *testing.B) Pool {
	pool, err := Create(poolPath, "", 10*1024*1024, 0666)
	if err != nil {
		b.Fatal(err)
	}
	return pool
}

func tearDown(p *Pool) {
	if p.open {
		p.Close()
	}
	os.Remove(poolPath)
}

func TestCreate(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)
}

func TestOpen(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)

	pool.Close()
	pool, err := Open(poolPath, "")
	if err != nil {
		t.Error(err)
	}
}

func TestReopen(t *testing.T) {
	var pool Pool
	var err error

	pool = helperCreateT(t)
	defer tearDown(&pool)

	for i := 0; i < 3; i++ {
		pool.Close()
		pool, err = Open(poolPath, "")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestOpenUnpermitted(t *testing.T) {
	pool, err := Create(poolPath, "", 10*1024*1024, 0000)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown(&pool)

	pool.Close()

	pool, err = Open(poolPath, "")

	if pool.pop != nil {
		t.Error("pop should be equal to nil")
	}
	if err == nil {
		t.Error("No permission denied error")
	}
}

func TestCreateTooSmall(t *testing.T) {
	pool, err := Create(poolPath, "", 1*1024, 0666)
	if pool.pop != nil {
		t.Error("pop should be equal to nil")
	}
	if err == nil {
		t.Error(err)
	}
	defer tearDown(&pool)
}

func TestAllocInt(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)

	b := blobInt{3}

	oid, err := pool.AllocBlob(&b)

	if err != nil {
		t.Errorf("%v. Oid: %v", err, oid)
	}
}
func TestReadInt(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)

	bIn := blobInt{888}
	oid, err := pool.AllocBlob(&bIn)
	if err != nil {
		t.Errorf("%v. Oid: %v", err, oid)
	}

	var bOut blobInt
	_, err = pool.FirstBlob(&bOut)
	if err != nil {
		t.Fatal(err)
	}

	if bOut.data != bIn.data {
		t.Errorf("Val is equal %d but should be %d", bOut.data, bIn.data)
	}
}

func TestReadStruct(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)

	bIn := blobStruct{myStruct{888, "mystring"}}
	oid, err := pool.AllocBlob(&bIn)
	if err != nil {
		t.Errorf("%v. Oid: %v", err, oid)
	}

	var bOut blobStruct
	_, err = pool.FirstBlob(&bOut)
	if err != nil {
		t.Fatal(err)
	}

	if bOut.my != bIn.my {
		t.Errorf("Val is equal %v but should be %v", bOut.my, bIn.my)
	}
}

func TestReadEmpty(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)

	_, err := pool.FirstBlob(nil)

	if err == nil {
		t.Error("No error despite reading from empty pool")
	}
}

func TestReadByNext(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)
	var b blobInt
	var err error

	vals := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for _, v := range vals {
		b.data = v
		_, err = pool.AllocBlob(&b)
		if err != nil {
			t.Fatal(err)
		}
	}

	var oid Oid
	for i, v := range vals {
		if i == 0 {
			oid, err = pool.FirstBlob(&b)
		} else {
			oid, err = oid.NextBlob(&b)
		}
		if err != nil {
			t.Fatal(err)
		}
		if b.data != v {
			t.Errorf("Val is equal %v but should be %v", b.data, v)
		}
	}
}

func TestReadEmptyByNext(t *testing.T) {
	pool := helperCreateT(t)
	defer tearDown(&pool)
	var b blobInt
	var err error

	numAllocs := 3
	for i := 0; i <= numAllocs; i++ {
		b.data = i
		_, err = pool.AllocBlob(&b)
		if err != nil {
			t.Fatal(err)
		}
	}

	var oid Oid
	for i := 0; i <= numAllocs+1; i++ {
		if i == 0 {
			oid, err = pool.FirstBlob(&b)
		} else {
			oid, err = oid.NextBlob(&b)
		}
		if i == numAllocs+1 {
			if err == nil {
				t.Error("Reading non-existent value should have returned error")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if b.data != i {
				t.Errorf("Val is equal %v but should be %v", b.data, i)
			}
		}
	}
}

func BenchmarkCreate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		pool, err := Create(poolPath, "", 10*1024*1024, 0666)

		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		tearDown(&pool)
		b.StartTimer()
	}
}

func BenchmarkOpen(b *testing.B) {
	pool := helperCreateB(b)
	defer tearDown(&pool)
	pool.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool, err := Open(poolPath, "")

		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}
		pool.Close()

		b.StartTimer()
	}
}

func BenchmarkAlloc(b *testing.B) {
	pool := helperCreateB(b)
	defer tearDown(&pool)
	blob := blobInt{999}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := pool.AllocBlob(&blob)
		if err != nil {
			b.Fatal(err)
		}
	}
}
