/*
Package nutz is a helper for interactions with bolt database. It makes messing with
boltdb buckets really fun. Just as bolt database, it doesnt inted to be an orm, just
buckets, buckets madness.
*/
package nutz


import (
    "errors"
    "github.com/boltdb/bolt"
    "os"
    "fmt"
)

type Storage struct {
    Data     []byte
    DataList map[string][]byte
    Error    error

    DBName string
    mode   os.FileMode
    DB     *bolt.DB
    Opts *bolt.Options
}

type storageFunc func(s Storage, bucket, key string, value []byte, nested ...string) Storage

func NewStorage(dbname string, mode os.FileMode, opts *bolt.Options) Storage {
    return Storage{
        DBName: dbname,
        mode:   mode,
        Opts:opts,
    }
}

func (s Storage) CreateDataRecord(bucket, key string, value []byte, nested ...string) Storage {
    return s.execute(bucket, key, value, nested, createDataRecord)
}

func createDataRecord(s Storage, bucket, key string, value []byte, nested ...string) Storage {
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
        s.Data = nil // Make sure no previous data is reurned
    }
    return s
}

func (s Storage) GetDataRecord(bucket, key string, nested ...string) Storage {
    return s.execute(bucket, key, nil, nested, getDataRecord)
}

func getDataRecord(s Storage, bucket, key string, value []byte, buckets ...string) Storage {
    var uerr error = nil
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
                return notFound("key",key)
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
            return notFound("bucket",bucket)
        }
        prev = b
        for i := 0; i < len(buckets); i++ {
            curr := prev.Bucket([]byte(buckets[i]))
            if curr == nil {
                uerr=notFound("bucket", buckets[i])
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
        s.Data = nil // Make sure no previous data is reurned
    }
    return s
}

func (s Storage) UpdateDataRecord(bucket, key string, value []byte, nested ...string) Storage {
    return s.execute(bucket, key, value, nested, updateDataRecord)
}

func updateDataRecord(s Storage, bucket, key string, value []byte, nested ...string) Storage {
    var uerr error
    if len(nested) == 0 {
        s.Error = s.DB.Update(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte(bucket))
            if b == nil {
                return notFound("bucket",bucket)
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
            return notFound("nucket",bucket)
        }
        prev = b
        for i := 0; i < len(nested); i++ {
            curr := prev.Bucket([]byte(nested[i]))
            if curr == nil {
                uerr=notFound("bucket",nested[i])
                break
            }
            prev = curr
        }
        if uerr != nil {
            return uerr
        }
        gkey := prev.Get([]byte(key))
        if gkey == nil {
            return notFound("key",key)
        }
        return prev.Put([]byte(key), value)
    })
    s.Data = value
    if s.Error != nil {
        s.Data = nil
    }
    return s
}

func (s Storage) GetAll(bucket string, nested ...string) Storage {
    return s.execute(bucket, "", nil, nested, getAll)
}

func getAll(s Storage, bucket, key string, value []byte, nested ...string) Storage {
    var uerr error = nil
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
                uerr=notFound("bucket",nested[i])
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

func (s Storage) RemoveDataRecord(bucket, key string, nested ...string) Storage {
    return s.execute(bucket, key, nil, nested, removeDataRecord)
}

func removeDataRecord(s Storage, bucket, key string, value []byte, nested ...string) Storage {
    var uerr error = nil
    if len(nested) == 0 {
        s.Error = s.DB.Update(func(tx *bolt.Tx) error {
            b := tx.Bucket([]byte(bucket))
            if b == nil {
                return notFound("bucket",bucket)
            }
            return b.Delete([]byte(key))
        })
        return s
    }
    s.Error = s.DB.Update(func(tx *bolt.Tx) error {
        var prev *bolt.Bucket
        b := tx.Bucket([]byte(bucket))
        if b == nil {
            return notFound("bucket",bucket)
        }
        prev = b
        for i := 0; i < len(nested); i++ {
            curr := prev.Bucket([]byte(nested[i]))
            if curr == nil {
                uerr=notFound("bucket",nested[i])
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
func (s Storage) DeleteDatabase() error {
    return os.Remove(s.DBName)
}
func (s Storage) execute(bucket, key string, value []byte, nested []string, fn storageFunc) Storage {
    s.DB, s.Error = bolt.Open(s.DBName, s.mode, nil)
    if s.Error != nil {
        panic(s.Error)
    }
    defer s.DB.Close()
    return fn(s, bucket, key, value, nested...)
}

func notFound(kind, msg string)error{
    return errors.New(fmt.Sprintf("nutz: %s  %s  not found",kind,msg))
}