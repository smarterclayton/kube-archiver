// Package archiver creates a record of the changes to the OpenShift and
// Kubernetes schema based on the underlying etcd event log. It also maintains
// secondary indices on other access patterns:
//
// * Event log - tracks creates, updates, and deletes
//   (etcdIndex, resourceType, namespace, name) -> [lastEtcdIndex, contents] (create/update) or [lastEtcdIndex] (delete)
//   Supports efficient in-order traversal of the change log
//
// * Resources by version
//   (resourceType, namespace, name, etcdIndex) -> []
//   Supports efficient retrieval of all historical versions of a resource
//
// * Resources by uid
//   (uid) -> [etcdIndex] or [etcdIndex, deletedEtcdIndex] (if deleted)
//   Supports efficient retrieval of resources by uid
//
// * Deleted resources in a namespace
//   (resourceType, namespace, deletedEtcdIndex, uid) -> [lastEtcdIndex]
//   Supports efficient retrieval of the deleted resources in a namespace in order
//
package bolt

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/boltdb/bolt"

	"github.com/openshift/kube-archiver/pkg/archiver/tuple"
)

var bucketEvents = tuple.Key("events")

func OpenBoltArchiver(path string, mode os.FileMode) (*BoltArchiver, error) {
	db, err := bolt.Open(path, mode, nil)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketEvents)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return NewBoltArchiver(db), nil
}

type BoltArchiver struct {
	db *bolt.DB
}

func NewBoltArchiver(db *bolt.DB) *BoltArchiver {
	return &BoltArchiver{db}
}

func (a *BoltArchiver) Close() error {
	return a.db.Close()
}

func (a *BoltArchiver) Dump(w io.Writer) error {
	return a.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketEvents)
		bucket.ForEach(func(k, v []byte) error {
			if t, err := tuple.Unpack(k); err == nil {
				fmt.Fprintf(w, "[%s]=%d\n", t, len(v))
			} else {
				fmt.Fprintf(w, "[%v]=%d\n", err, len(v))
			}
			return nil
		})
		return nil
	})
}

func (a *BoltArchiver) Create(resource, namespace, name, uid string, index uint64, current []byte) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketEvents)

		if err := bucket.Put(keyByIndex(resource, namespace, name, index, 0).Pack(), current); err != nil {
			return err
		}

		if err := bucket.Put(keyByType(resource, namespace, name, index).Pack(), nil); err != nil {
			return err
		}

		if len(uid) != 0 {
			if err := bucket.Put(tuple.Tuple{uid}.Pack(), tuple.Tuple{index}.Pack()); err != nil {
				return err
			}
		}

		return nil
	})
}

func (a *BoltArchiver) Update(resource, namespace, name, uid string, index uint64, current []byte) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketEvents)

		if err := bucket.Put(keyByIndex(resource, namespace, name, index, 1).Pack(), current); err != nil {
			return err
		}

		prefix := keyByType(resource, namespace, name, index)[:2]
		if err := deleteRange(bucket, prefix.Pack()); err != nil {
			return err
		}
		if err := bucket.Put(keyByType(resource, namespace, name, index).Pack(), nil); err != nil {
			return err
		}

		if len(uid) != 0 {
			if err := bucket.Put(tuple.Tuple{uid}.Pack(), tuple.Tuple{index}.Pack()); err != nil {
				return err
			}
		}

		return nil
	})
}

func (a *BoltArchiver) Delete(resource, namespace, name, uid string, index uint64) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketEvents)

		if err := bucket.Put(keyByIndex(resource, namespace, name, index, 2).Pack(), []byte{}); err != nil {
			return err
		}

		prefix := keyByType(resource, namespace, name, index)[:2]
		if err := deleteRange(bucket, prefix.Pack()); err != nil {
			return err
		}

		if len(uid) != 0 {
			if err := bucket.Delete(tuple.Tuple{uid}.Pack()); err != nil {
				return err
			}
		}

		return nil
	})
}

func deleteRange(bucket *bolt.Bucket, prefix tuple.Key) error {
	cursor := bucket.Cursor()
	for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
		if err := bucket.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

func keyByIndex(resource, namespace, name string, index uint64, update byte) tuple.Tuple {
	return tuple.Tuple{index, resource, namespace, name, []byte{update}}
}

func keyByType(resource, namespace, name string, index uint64) tuple.Tuple {
	return tuple.Tuple{resource, namespace, name, index}
}
