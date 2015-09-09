package kv

import (
	"strings"
)

type KeyMapper interface {
	KeyForPath(path string) (resource string, namespace string, name string, ok bool)
}

type PrefixMap map[string]map[string]struct {
	Namespaced   bool
	ResourceName string
}

func (m PrefixMap) KeyForPath(path string) (resource string, namespace string, name string, ok bool) {
	segments := strings.SplitN(path[1:], "/", 4)
	if len(segments) < 2 {
		return
	}
	prefix := segments[0]
	segments = segments[1:]

	resource, name, namespace = segments[0], "", ""
	base, ok := PrefixMap(m)[prefix]
	if !ok {
		return
	}
	t, ok := base[resource]
	if !ok {
		return
	}
	if t.Namespaced {
		if len(segments) != 3 {
			return
		}
		namespace, name = segments[1], segments[2]
		ok = true
	} else {
		if len(segments) != 2 {
			return
		}
		name = segments[1]
		ok = true
	}
	if len(t.ResourceName) > 0 {
		resource = t.ResourceName
	}
	return
}
