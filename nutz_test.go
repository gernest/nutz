package nutz

import (
	"bytes"
	"testing"
)

var (
	tData = []struct {
		key     string
		value   []byte
		buckets []string
	}{
		{"moja", []byte("moja"), []string{"one"}},
		{"mbili", []byte("mbili"), []string{"one", "two"}},
		{"tatu", []byte("tatu"), []string{"one", "two", "three"}},
	}
	s      = NewStorage("storage_test.db", 0600, nil)
	single = struct {
		bucket string
		key    string
		value  []byte
	}{"single", "single", []byte("single")}
	base = "base"
	nest = []string{"solo", "bele", "gaza"}
)

func Test_Create(t *testing.T) {
	//defer s.DeleteDatabase()

	for _, v := range tData {
		err := s.Create(v.buckets[0], v.key, v.value, v.buckets...)
		if err.Error != nil {
			t.Errorf("creating records: %v", err.Error)
		}
	}
	err := s.Create(single.bucket, single.key, single.value)
	if err.Error != nil {
		t.Errorf("creating a non nested record: %v", err.Error)
	}
	err = s.Create("", tData[0].key, tData[0].value)
	if err.Error == nil {
		t.Errorf("expected nil got %v", err.Error)
	}
	err = s.Create("", tData[0].key, tData[0].value, tData[0].buckets...)
	if err.Error == nil {
		t.Errorf("expected nil got %v", err.Error)
	}
	err = s.Create("correct", tData[0].key, tData[0].value, "", "")
	if err.Error == nil {
		t.Errorf("expected an error got %v", err.Error)
	}
	err = s.Create("babel", "", tData[0].value, tData[0].buckets...)
	if err.Error == nil {
		t.Errorf("expected an error got %v", err.Error)
	}
}

func Test_Get(t *testing.T) {
	for _, v := range tData {
		err := s.Get(v.buckets[0], v.key, v.buckets...)
		if err.Error != nil {
			t.Errorf("getting records: %v", err.Error)
		}
		if !bytes.Equal(err.Data, v.value) {
			t.Errorf("getting records: expected %s got %s", string(v.value), string(err.Data))
		}
	}

	err := s.Get(single.bucket, single.key)
	if err.Error != nil {
		t.Errorf("getting a non nested record: %v", err.Error)
	}
	err = s.Get("", single.key)
	if err.Error == nil {
		t.Errorf("getting a non nested record:expected an error got %v", err.Error)
	}
	err = s.Get(single.bucket, "")
	if err.Error == nil {
		t.Errorf("getting a non nested record:expected an error got %v", err.Error)
	}
	err = s.Get("", tData[0].key, tData[0].buckets...)
	if err.Error == nil {
		t.Errorf("getting a non nested record:expected an error got %v", err.Error)
	}
	err = s.Get(tData[0].buckets[0], tData[0].key, "", "")
	if err.Error == nil {
		t.Errorf("getting a non nested record:expected an error got %v", err.Error)
	}
	err = s.Get(tData[0].buckets[0], "", tData[0].buckets...)
	if err.Error == nil {
		t.Errorf("getting a non nested record:expected an error got %v", err.Error)
	}
}

func Test_Update(t *testing.T) {
	for _, v := range tData {
		err := s.Update(v.buckets[0], v.key, []byte(v.buckets[0]), v.buckets...)
		if err.Error != nil {
			t.Errorf("updating records: %v", err.Error)
		}
		g := s.Get(v.buckets[0], v.key, v.buckets...)
		if !bytes.Equal(g.Data, []byte(v.buckets[0])) {
			t.Errorf("getting records: expected %s got %s", v.buckets[0], string(g.Data))
		}
	}
	u := []byte("up-to-date")
	err := s.Update(single.bucket, single.key, u)
	if err.Error != nil {
		t.Errorf("updating records: %v", err.Error)
	}
	err = s.Update("", tData[0].key, tData[0].value)
	if err.Error == nil {
		t.Errorf("expected nil got %v", err.Error)
	}
	err = s.Update(single.bucket, "", u)
	if err.Error == nil {
		t.Errorf("expected nil got %v", err.Error)
	}
	err = s.Update("", tData[0].key, u, tData[0].buckets...)
	if err.Error == nil {
		t.Errorf("expected nil got %v", err.Error)
	}
	err = s.Update(tData[0].buckets[0], tData[0].key, u, "", "")
	if err.Error == nil {
		t.Errorf("expected an error got %v", err.Error)
	}
}

func Test_GetAll(t *testing.T) {
	base := "base"
	nest := []string{"solo", "bele", "gaza"}
	for _, v := range tData {
		err := s.Create(base, v.key, v.value)
		if err.Error != nil {
			t.Errorf("creating records: %v", err.Error)
		}
	}
	a := s.GetAll(base)
	if a.Error != nil {
		t.Errorf("getting all records: %v", a.Error)
	}
	if len(a.DataList) != 3 {
		t.Errorf("expected 3 got %d", len(a.DataList))
	}
	err := s.GetAll("")
	if err.Error == nil {
		t.Errorf("expected an error got %v", err.Error)
	}

	for _, v := range tData {
		err := s.Create(base, v.key, v.value, nest...)
		if err.Error != nil {
			t.Errorf("creating records: %v", err.Error)
		}
	}
	b := s.GetAll(base, nest...)
	if b.Error != nil {
		t.Errorf("getting all records: %v", b.Error)
	}
	if len(b.DataList) != 3 {
		t.Errorf("expected 3 got %d", len(b.DataList))
	}
	err = s.GetAll("", nest...)
	if err.Error == nil {
		t.Errorf("expected an error got %v", err.Error)
	}
	err = s.GetAll(base, "")
	if err.Error == nil {
		t.Errorf("expected an error got %v", err.Error)
	}
}

func Test_Delete(t *testing.T) {
	defer s.DeleteDatabase()

	err := s.Delete(single.bucket, single.key)
	if err.Error != nil {
		t.Errorf("deleting records %v", err.Error)
	}

	err = s.Delete("", single.key)
	if err.Error == nil {
		t.Errorf("expected nil got %v", err.Error)
	}

	for _, v := range tData {
		d := s.Delete(base, v.key, nest...)
		if d.Error != nil {
			t.Errorf("deleting records: %v", d.Error)
		}
		derr := s.Delete("", v.key, nest...)
		if derr.Error == nil {
			t.Errorf("expected error got %v", derr.Error)
		}
		derr = s.Delete(base, v.key, "")
		if derr.Error == nil {
			t.Errorf("expected error got %v", derr.Error)
		}
	}
}

func Test_Execute(t *testing.T) {
	defer s.DeleteDatabase()
	err := s.Execute("home", "myKey", []byte("byVal"), []string{}, create)
	if err.Error != nil {
		t.Errorf("expected nil got %v", err.Error)
	}
	z := NewStorage("", 0600, nil)
	err = z.Execute("home", "myKey", []byte("byVal"), []string{}, create)
	if err.Error == nil {
		t.Errorf("expected error got %v", err.Error)
	}
}

func ExampleStorage_Create() {
	c := NewStorage("mydatabase.bdb", 0600, nil)

	// Without nesting
	c.Create("my-bucket", "my-key", []byte("my-value"))

	// With nested buckets
	c.Create("my-bucket", "my-key", []byte("my-value"), "bucket-one", "bucket-two")

	// When you nest buckets, the order of the optional coma separated strings matter.
	// That is, on the above code snippet.
	//  * A bucket named "my-bucket" will be created"
	//  * Inside "my-bucket" a bucket "bucket-one" will be created.
	//  * Inside "bucket-one" a bucket "bucket-two" will be created.
	//  * Inside "bucket-two" a new record will be stored with key "my-key" and value "my-value"
}
