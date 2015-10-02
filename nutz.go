/*
Package nutz is a helper for interactions with bolt databases.
*/
package nutz

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

// Storage contains metadata, which enable accessing the underlying bolt databases.
type Storage struct {
	// Data holds the value returned from the database.
	Data []byte

	// DataList holds key value pairs, retrievend by the GetAll method. These are the
	// values found in the bucket.
	DataList map[string][]byte

	// Error stores the last encountered error.
	Error error

	// DBName filename which will be used as the database.
	DBName string
	mode   os.FileMode
	db     *bolt.DB
	opts   *bolt.Options
}

// StorageFunc is the interface for a function which is used by Execute method
type StorageFunc func(s Storage, bucket, key string, value []byte, nested ...string) Storage

// NewStorage initializes a new storage object
func NewStorage(dbname string, mode os.FileMode, opts *bolt.Options) Storage {
	return Storage{
		DBName: dbname,
		mode:   mode,
		opts:   opts,
	}
}

// Create stores a given key value pairs . It takes an optional coma
// separated strings to act as nested buckets.
func (s Storage) Create(bucket, key string, value []byte, nested ...string) Storage {
	return s.execute(bucket, key, value, nested, create)
}

func create(s Storage, bucket, key string, value []byte, nested ...string) Storage {
	if len(nested) == 0 {
		s.Error = s.db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return err
			}
			return b.Put([]byte(key), value)
		})
	} else {
		s.Error = s.db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(bucket))
			if err != nil {
				return err
			}
			prev, perr := createNestedBuckets(nested, b)
			if perr != nil {
				return perr
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
	}

	if s.Error != nil {
		s.Data = nil // Make sure no previous data is returned
	}
	return s
}

// Get retrives a record from the database. The order of the optional nested buckets matter.
// if a key does not exist it returns an error
func (s Storage) Get(bucket, key string, nested ...string) Storage {
	return s.execute(bucket, key, nil, nested, getData)
}

func getData(s Storage, bucket, key string, value []byte, buckets ...string) Storage {
	if len(buckets) == 0 {
		s.Error = s.db.View(func(tx *bolt.Tx) error {
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
	} else {
		s.Error = s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return notFound("bucket", bucket)
			}
			prev, perr := getNestedBucket(buckets, b)
			if perr != nil {
				return perr
			}

			rst := prev.Get([]byte(key))
			if rst == nil {
				return bolt.ErrBucketNotFound
			}
			s.Data = make([]byte, len(rst))
			copy(s.Data, rst)
			return nil
		})
	}

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
	if len(nested) == 0 {
		s.Error = s.db.Update(func(tx *bolt.Tx) error {
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
	} else {
		s.Error = s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return notFound("nucket", bucket)
			}
			prev, perr := getNestedBucket(nested, b)
			if perr != nil {
				return perr
			}
			return prev.Put([]byte(key), value)
		})
		s.Data = value
	}
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
	s.DataList = make(map[string][]byte)
	if len(nested) == 0 {
		s.Error = s.db.View(func(tx *bolt.Tx) error {
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
	} else {
		s.Error = s.db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return notFound("bucket", bucket)
			}
			prev, perr := getNestedBucket(nested, b)
			if perr != nil {
				return perr
			}
			return prev.ForEach(func(k, v []byte) error {
				dv := make([]byte, len(v))
				copy(dv, v)
				s.DataList[string(k)] = dv
				return nil
			})

		})
	}

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
	if len(nested) == 0 {
		s.Error = s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return notFound("bucket", bucket)
			}
			return b.Delete([]byte(key))
		})
		s.Data = []byte(key)
	} else {
		s.Error = s.db.Update(func(tx *bolt.Tx) error {
			var prev *bolt.Bucket
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return notFound("bucket", bucket)
			}
			prev, perr := getNestedBucket(nested, b)
			if perr != nil {
				return perr
			}

			return prev.Delete([]byte(key))
		})
		s.Data = []byte(key)
	}

	if s.Error != nil {
		s.Data = nil // Make sure no previous data is reurned
	}
	return s
}

// DeleteDatabase removes the given database file as specified in the NewStorage function
// from the filesystem
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
	s.db, s.Error = bolt.Open(s.DBName, s.mode, s.opts)
	if s.Error != nil {
		return s
	}
	defer s.db.Close()
	return fn(s, bucket, key, value, nested...)
}

func notFound(kind, msg string) error {
	return fmt.Errorf("nutz: %s  %s  not found", kind, msg)
}

func getNestedBucket(n []string, b *bolt.Bucket) (*bolt.Bucket, error) {
	var prev *bolt.Bucket
	var uerr error
	prev = b
	for i := 0; i < len(n); i++ {
		curr := prev.Bucket([]byte(n[i]))
		if curr == nil {
			uerr = notFound("bucket", n[i])
			break
		}
		prev = curr
	}
	return prev, uerr
}

func createNestedBuckets(n []string, b *bolt.Bucket) (*bolt.Bucket, error) {
	var (
		prev, curr *bolt.Bucket
		err        error
	)
	prev = b
	for i := 0; i < len(n); i++ {
		if i == len(n)-1 {
			curr, err = prev.CreateBucket([]byte(n[i]))
		} else {
			curr, err = prev.CreateBucketIfNotExists([]byte(n[i]))
		}
		if err != nil {
			break
		}
		prev = curr
	}
	return prev, err
}
