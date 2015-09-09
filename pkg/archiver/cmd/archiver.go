package cmd

import (
	"fmt"
	"os"
	"time"

	//"github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
	"github.com/spf13/cobra"

	"github.com/openshift/kube-archiver/pkg/archiver/bolt"
	"github.com/openshift/kube-archiver/pkg/archiver/source/etcd"
	"github.com/openshift/kube-archiver/pkg/archiver/source/kv"
	"github.com/openshift/kube-archiver/pkg/cmd/util/etcdcmd"
)

const longCommandDesc = `
Start an OpenShift archiver

This command launches an archiver connected to your etcd store. The archiver copies changes to
etcd into a backing store that can provide historical state over OpenShift and Kubernetes
resources
`

type config struct {
	Config         *etcdcmd.Config
	DeploymentName string
}

func NewCommandArchiver(name string) *cobra.Command {
	cfg := &config{
		Config: etcdcmd.NewConfig(),
	}

	cmd := &cobra.Command{
		Use:   fmt.Sprintf("%s%s", name, etcdcmd.ConfigSyntax),
		Short: "Start an OpenShift archiver",
		Long:  longCommandDesc,
		Run: func(c *cobra.Command, args []string) {
			if err := start(cfg); err != nil {
				glog.Fatal(err)
			}
		},
	}

	flag := cmd.Flags()
	cfg.Config.Bind(flag)

	return cmd
}

// start begins archiving
func start(cfg *config) error {
	client, err := cfg.Config.Client(true)
	if err != nil {
		return err
	}

	// initialize the handlers
	archiver, err := bolt.OpenBoltArchiver("openshift-archive.boltdb", 0640)
	if err != nil {
		return err
	}
	defer archiver.Close()

	source := etcd.NewSource(client, archiver, kv.PrefixMap{
		"openshift.io": {
			"buildconfigs":           {Namespaced: true},
			"deploymentconfigs":      {Namespaced: true},
			"replicationcontrollers": {Namespaced: true},
			"imagestreams":           {Namespaced: true},
			"images":                 {},
		},
		"kubernetes.io": {
			"namespaces":  {},
			"controllers": {Namespaced: true, ResourceName: "replicationcontrollers"},
			"pods":        {Namespaced: true},
		},
	})

	if err := source.Run(); err != nil {
		return err
	}
	for _ = range time.NewTicker(5 * time.Second).C {
		archiver.Dump(os.Stdout)
	}

	select {}
	return nil
}
