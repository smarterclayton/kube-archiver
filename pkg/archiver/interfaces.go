package archiver

import (
	"github.com/golang/glog"
)

type Interface interface {
	Create(resource, namespace, name, uid string, index uint64, current []byte) error
	Update(resource, namespace, name, uid string, index uint64, current []byte) error
	Delete(resource, namespace, name, uid string, index uint64) error
}

type LogArchiver struct{}

func (a *LogArchiver) Create(resource, namespace, name, uid string, index uint64, current []byte) error {
	glog.Infof("created %s %q %q %q %d", resource, namespace, name, uid, index)
	return nil
}

func (a *LogArchiver) Update(resource, namespace, name, uid string, index uint64, current []byte) error {
	glog.Infof("updated %s %q %q %q %d", resource, namespace, name, uid, index)
	return nil
}

func (a *LogArchiver) Delete(resource, namespace, name, uid string, index uint64) error {
	glog.Infof("deleted %s %q %q %q %d", resource, namespace, name, uid, index)
	return nil
}
