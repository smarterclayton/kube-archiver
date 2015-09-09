package bolt

import (
	//"bytes"
	//"reflect"

	"github.com/boltdb/bolt"
	//"k8s.io/kubernetes/pkg/conversion"
	"k8s.io/kubernetes/pkg/runtime"
	"k8s.io/kubernetes/pkg/storage"

	//"github.com/openshift/kube-archiver/pkg/archiver/tuple"
)

type ResourceAccessor struct {
	DB    *bolt.DB
	Codec runtime.Codec

	// optional, no atomic operations can be performed without this interface
	ResourceVersioner storage.Versioner
}

/*func (a *ResourceAccessor) ExtractVersionList(resource, namespace, name string, slicePtr interface{}, resourceVersion *uint64,
  minVersion uint64, limit int) error {

  out, err := conversion.EnforcePtr(slicePtr)
  if err != nil || out.Kind() != reflect.Slice {
    // This should not happen at runtime.
    panic("need ptr to slice")
  }

  return a.DB.View(func(tx *bolt.Tx) error {
    cursor := tx.Bucket(bucketEvents).Cursor()
    prefix := tuple.Tuple{resource, namespace, name}
    seek := prefix
    if minVersion != 0 {
      seek = append(seek, minVersion)
    }

    count := 0
    for k, _ := cursor.Seek(seek.Pack()); k != nil && bytes.HasPrefix(k, prefix.Pack()); k, _ = cursor.Next() {
      t, err := tuple.Unpack(k)
      if err != nil {
        return err
      }
      index := t[len(t)-1]

      obj := reflect.New(out.Type().Elem())
      if err := a.rawExtractVersion(tx, resource, namespace, name, index, &obj, false); err != nil {
        // versions that are missing from the version store are omitted
        continue
      }
      out.Set(reflect.Append(out, obj.Elem()))

      count += 1
      if count >= limit {
        break
      }
    }
  })
}

// rawExtractVersion returns a version that has been persisted into the event store into objPtr. If
// ignoreNotFound is true, a new unset object will be returned.
func (a *ResourceAccessor) rawExtractVersion(tx *bolt.Tx,
  resource, namespace, name string, index uint64,
  objPtr runtime.Object, ignoreNotFound bool) error {

  bucket := tx.Bucket(bucketEvents)
  key := tuple.Tuple{index, resource, namespace, name}
  value := bucket.Get(prefix)
  if value == nil && !ignoreNotFound {
    return errors.New("not found")
  }
  err := a.Codec.DecodeInto(value, objPtr)
  if a.ResourceVersioner != nil {
    _ = a.ResourceVersioner.SetResourceVersion(objPtr, index)
    // being unable to set the version does not prevent the object from being extracted
  }
  return err
}

/*
func (a *BoltArchiver) Update(resource, namespace, name string, index uint64, current *etcd.Node, previous *etcd.Node) error {
  uid, ok := extractUID(current.Value)
  return a.db.Update(func(tx *bolt.Tx) error {
    bucket := tx.Bucket(bucketEvents)

    if err := bucket.Put(keyByIndex(resource, namespace, name, index).Pack(), tuple.Tuple{previous.ModifiedIndex, current.Value}.Pack()); err != nil {
      return err
    }

    prefix := keyByType(resource, namespace, name, index)[:2]
    if err := deleteRange(bucket, prefix.Pack()); err != nil {
      return err
    }
    if err := bucket.Put(keyByType(resource, namespace, name, index).Pack(), nil); err != nil {
      return err
    }

    if ok {
      if err := bucket.Put([]byte(uid), tuple.Tuple{index}.Pack()); err != nil {
        return err
      }
    }

    return nil
  })
}
*/
