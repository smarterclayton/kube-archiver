package etcd

import (
	"fmt"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/util"
	errutil "k8s.io/kubernetes/pkg/util/errors"

	"github.com/openshift/kube-archiver/pkg/archiver"
	"github.com/openshift/kube-archiver/pkg/archiver/source"
	"github.com/openshift/kube-archiver/pkg/archiver/source/kv"
)

type Source struct {
	client   *etcd.Client
	archiver archiver.Interface
	mapper   kv.KeyMapper
}

func NewSource(client *etcd.Client, archiver archiver.Interface, mapper kv.KeyMapper) *Source {
	return &Source{
		client:   client,
		archiver: archiver,
		mapper:   mapper,
	}
}

func (s *Source) Run() error {
	// locate the oldest snapshot
	snapshotSize := uint64(1000)
	snapshotWindow := snapshotSize
	resp, err := s.client.Get("/", false, false)
	if err != nil {
		return err
	}
	recentIndex := uint64(1)
	if resp.EtcdIndex > snapshotSize {
		recentIndex = resp.EtcdIndex - snapshotWindow + 1
	}

	watches := make(chan chan *etcd.Response)

	go util.Forever(func() {
		ch := make(chan *etcd.Response)
		watches <- ch
		if _, err := s.client.Watch("/", recentIndex, true, ch, nil); err != nil {
			snapshotWindow = snapshotWindow * 9 / 10
			if etcdError, ok := err.(*etcd.EtcdError); ok {
				recentIndex = etcdError.Index - snapshotWindow
			}
			glog.Errorf("Unable to watch: %v", err)
			return
		}
		snapshotWindow = snapshotSize
	}, 1*time.Second)

	lowestIndex := uint64(0)
	go util.Forever(func() {
		glog.Infof("Ready to archive changes from etcd ...")

		for ch := range watches {
			glog.Infof("Watching ...")
			for resp := range ch {
				index, err := s.OnEvent(resp)
				if err != nil {
					glog.Errorf("error: %v", err)
					continue
				}
				if index == 0 {
					break
				}
				lowestIndex = index
			}
		}
	}, 10*time.Millisecond)
	return nil
}

func (s *Source) OnEvent(resp *etcd.Response) (uint64, error) {
	var path string
	var index uint64
	switch {
	case resp.Node != nil:
		path = resp.Node.Key
		index = resp.Node.ModifiedIndex
	case resp.PrevNode != nil:
		path = resp.PrevNode.Key
		index = resp.Node.ModifiedIndex
	default:
		return resp.EtcdIndex, nil
	}
	resource, namespace, name, ok := s.mapper.KeyForPath(path)
	if !ok {
		glog.V(4).Infof("Ignoring path %s", path)
		return index, nil
	}

	switch resp.Action {
	// expect both nodes to be set for deletion
	case "expire", "delete", "compareAndDelete":
		if resp.PrevNode == nil || resp.Node == nil {
			return index, fmt.Errorf("event %s at %d had no previous node, cannot record deletion", resp.Action, resp.EtcdIndex)
		}
		uid, _ := source.ExtractUID([]byte(resp.PrevNode.Value))
		return index, s.archiver.Delete(resource, namespace, name, uid, resp.Node.ModifiedIndex)

	// extract a create or update
	case "create", "set", "compareAndSwap":
		switch {
		case resp.PrevNode != nil && resp.Node != nil:
			errs := []error{}
			prevNode := resp.PrevNode
			prevValue := []byte(prevNode.Value)
			uid, _ := source.ExtractUID(prevValue)
			if prevNode.CreatedIndex != prevNode.ModifiedIndex {
				if err := s.archiver.Update(resource, namespace, name, uid, prevNode.ModifiedIndex, prevValue); err != nil {
					errs = append(errs, err)
				}
			} else {
				if err := s.archiver.Create(resource, namespace, name, uid, prevNode.ModifiedIndex, prevValue); err != nil {
					errs = append(errs, err)
				}
			}

			value := []byte(resp.Node.Value)
			uid, _ = source.ExtractUID(value)
			if err := s.archiver.Update(resource, namespace, name, uid, resp.Node.ModifiedIndex, value); err != nil {
				errs = append(errs, err)
			}
			return index, errutil.NewAggregate(errs)

		// can only be a create
		case resp.Node != nil:
			value := []byte(resp.Node.Value)
			uid, _ := source.ExtractUID(value)
			return index, s.archiver.Create(resource, namespace, name, uid, resp.Node.CreatedIndex, value)

		default:
			return index, fmt.Errorf("event %s at %d had no node", resp.Action, index)
		}
	case "get":
		// do nothing for quorum gets
	default:
		return index, fmt.Errorf("unrecognized etcd watch type %s at %d", resp.Action, resp.EtcdIndex)
	}
	return index, nil
}
