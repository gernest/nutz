/*
Package nutz is a helper for interactions with bolt database. It makes messing with
boltdb buckets really fun, just
buckets, buckets madness.
*/
package nutz

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

// Storage where the action begins
type Storage struct {
	Data     []byte // Holds the data returned from the database
	DataList map[string][]byte
	Error    error

	DBName string
	mode   os.FileMode
	DB     *bolt.DB
	Opts   *bolt.Options
}

// StorageFunc is the interface for a function which is used by Execute method
type StorageFunc func(s Storage, bucket, key string, value []byte, nested ...string) Storage

// NewStorage initializes a new storage object
func NewStorage(dbname string, mode os.FileMode, opts *bolt.Options) Storage {
	return Storage{
		DBName: dbname,
		mode:   mode,
		Opts:   opts,
	}
}

// Create stores a given key value pairs inside the buckets. If the buckets does not exist they will be created
// the first argument is the base, or root bucket to be used. It takes an optional list of bucket names which can be
// handy when you want to have a nested bucket structure.
//
//		// Lets say you want to store a key value pair pair, k1 and V1 into a nestended structure like
//		// b1,b2,b3. This means k1 will be in bucket b3, where bucket b3 is inside bucket b2 which in turn is
//		// inside bucket b1.
//		// You can do something like this
//		buckets:=[]string{"b2","b3"}
//		Create("b1",k1,v1,buckets...)
func (s Storage) Create(bucket, key string, value []byte, nested ...string) Storage {
	return s.execute(bucket, key, value, nested, create)
}

func create(s Storage, bucket, key string, value []byte, nested ...string) Storage {
	if len(nested) == 0 {
		s.Error = s.DB.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return err
			}
			if b == nil {
				return bolt.ErrBucketNotFound
			}
			return b.Put([]byte(key), value)
		})
		if s.Error == nil {
			s.Data = value
		}
		return s
	}
	s.Error = s.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var prev *bolt.Bucket
		prev = b
		for i := 0; i < len(nested); i++ {
			curr, err := prev.CreateBucketIfNotExists([]byte(nested[i]))
			if err != nil {
				break
			}
			if curr == nil {
				continue
			}
			prev = curr
		}

		err = prev.Put([]byte(key), value)
		if err != nil {
			return err
		}

		rst := prev.Get([]byte(key))
		if rst != nil {
			s.Data = make([]byte, len((rst)))
			copy(s.Data, rst)
		}
		return nil
	})
	if s.Error != nil {
		s.Data = nil // Make sure no previous data is returned
	}
	return s
}

// Get retrives a record from the database. The order of the optioal nested buckets matter.
// if a key does not exist it returns an error
func (s Storage) Get(bucket, key string, nested ...string) Storage {
	return s.execute(bucket, key, nil, nested, getData)
}

func getData(s Storage, bucket, key string, value []byte, buckets ...string) Storage {
	var uerr error
	if len(buckets) == 0 {
		s.Error = s.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return bolt.ErrBucketNotFound
			}
			res := b.Get([]byte(key))
			if res != nil {
				s.Data = make([]byte, len(res))
				copy(s.Data, res)
			}
			if res == nil {
				return notFound("key", key)
			}
			return nil
		})
		if s.Error != nil {
			s.Data = nil
		}
		return s
	}
	s.Error = s.DB.View(func(tx *bolt.Tx) error {
		var prev *bolt.Bucket
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return notFound("bucket", bucket)
		}
		prev = b
		for i := 0; i < len(buckets); i++ {
			curr := prev.Bucket([]byte(buckets[i]))
			if curr == nil {
				uerr = notFound("bucket", buckets[i])
				break
			}
			prev = curr
		}
		if uerr != nil {
			return uerr
		}

		rst := prev.Get([]byte(key))
		if rst == nil {
			return bolt.ErrBucketNotFound
		}
		s.Data = make([]byte, len(rst))
		copy(s.Data, rst)
		return nil
	})
	if s.Error != nil {
		s.Data = nil
	}
	return s
}

// Update replaces the old data stored in a given key with a new one. If the Key is not found
// it returns an error. Update does not attempt to create missing buckets, instead it returns an error
//
// This shares the same api as the Create method,
// except it returns an error if the record does not exist,
// and in case of buckets it returns a error if they dont exist.
//
// For a nested bucket list, and missing bucket in the list,
// or missarrangement result in an error.
func (s Storage) Update(bucket, key string, value []byte, nested ...string) Storage {
	return s.execute(bucket, key, value, nested, update)
}

func update(s Storage, bucket, key string, value []byte, nested ...string) Storage {
	var uerr error
	if len(nested) == 0 {
		s.Error = s.DB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return notFound("bucket", bucket)
			}
			gkey := b.Get([]byte(key))
			if gkey == nil {
				return notFound("key", key)
			}
			return b.Put([]byte(key), value)
		})
		s.Data = value
		if s.Error != nil {
			s.Data = nil
		}
		return s
	}
	s.Error = s.DB.Update(func(tx *bolt.Tx) error {
		var prev *bolt.Bucket
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return notFound("nucket", bucket)
		}
		prev = b
		for i := 0; i < len(nested); i++ {
			curr := prev.Bucket([]byte(nested[i]))
			if curr == nil {
				uerr = notFound("bucket", nested[i])
				break
			}
			prev = curr
		}
		if uerr != nil {
			return uerr
		}
		gkey := prev.Get([]byte(key))
		if gkey == nil {
			return notFound("key", key)
		}
		return prev.Put([]byte(key), value)
	})
	s.Data = value
	if s.Error != nil {
		s.Data = nil
	}
	return s
}

// GetAll retrieves all key value pairs in a bucket, it stores the map of key value pairs
// inside the DataList attribute.
func (s Storage) GetAll(bucket string, nested ...string) Storage {
	return s.execute(bucket, "", nil, nested, getAll)
}

func getAll(s Storage, bucket, key string, value []byte, nested ...string) Storage {
	var uerr error
	s.DataList = make(map[string][]byte)
	if len(nested) == 0 {
		err := s.DB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return bolt.ErrBucketNotFound
			}
			return b.ForEach(func(k, v []byte) error {
				dv := make([]byte, len(v))
				copy(dv, v)
				s.DataList[string(k)] = dv
				return nil
			})
		})

		s.Error = err
		return s
	}

	s.Error = s.DB.View(func(tx *bolt.Tx) error {
		var prev *bolt.Bucket
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return notFound("bucket", bucket)
		}
		prev = b
		for i := 0; i < len(nested); i++ {
			curr := prev.Bucket([]byte(nested[i]))
			if curr == nil {
				uerr = notFound("bucket", nested[i])
				break
			}
			prev = curr
		}

		if uerr != nil {
			return uerr
		}
		rerr := prev.ForEach(func(k, v []byte) error {
			dv := make([]byte, len(v))
			copy(dv, v)
			s.DataList[string(k)] = dv
			return nil
		})
		if rerr != nil {
			return rerr
		}
		return nil
	})
	if s.Error != nil {
		s.Data = nil // Make sure no previous data is reurned
	}
	return s
}

// Delete removes a record from the database
func (s Storage) Delete(bucket, key string, nested ...string) Storage {
	return s.execute(bucket, key, nil, nested, remove)
}

func remove(s Storage, bucket, key string, value []byte, nested ...string) Storage {
	var uerr error
	if len(nested) == 0 {
		s.Error = s.DB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return notFound("bucket", bucket)
			}
			return b.Delete([]byte(key))
		})
		return s
	}
	s.Error = s.DB.Update(func(tx *bolt.Tx) error {
		var prev *bolt.Bucket
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return notFound("bucket", bucket)
		}
		prev = b
		for i := 0; i < len(nested); i++ {
			curr := prev.Bucket([]byte(nested[i]))
			if curr == nil {
				uerr = notFound("bucket", nested[i])
				break
			}
			prev = curr
		}
		if uerr != nil {
			return uerr
		}
		perr := prev.Delete([]byte(key))
		if perr != nil {
			s.Data = nil
			return perr
		}
		s.Data = []byte(key)
		return nil

	})
	if s.Error != nil {
		s.Data = nil // Make sure no previous data is reurned
	}
	return s
}

// DeleteDatabase removes the given database file as specified in the NewStorage function
// from the disc
func (s Storage) DeleteDatabase() error {
	return os.Remove(s.DBName)
}

// Execute opens a database, and pass all arguments together with a Storage object
// that has an open bolt database to a given StorageFunc. This is an ideal way to tap into
// the *bolt.DB object and do the whole stuffs you would like do do, without even bothering
// if the database exist of if the connection was closed.
//
// It makes sure the database is closed after the function exits. Note that, the nested
// buckets should be provided as a slice of strings.
func (s *Storage) Execute(bucket, key string, value []byte, nested []string, fn StorageFunc) Storage {
	return s.execute(bucket, key, value, nested, fn)
}

func (s Storage) execute(bucket, key string, value []byte, nested []string, fn StorageFunc) Storage {
	s.DB, s.Error = bolt.Open(s.DBName, s.mode, nil)
	if s.Error != nil {
		panic(s.Error)
	}
	defer s.DB.Close()
	return fn(s, bucket, key, value, nested...)
}

func notFound(kind, msg string) error {
	return fmt.Errorf("nutz: %s  %s  not found", kind, msg)
}
