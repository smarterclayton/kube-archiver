package main

import (
	"os"

	"github.com/openshift/kube-archiver/pkg/archiver/cmd"
	"github.com/openshift/origin/pkg/cmd/flagtypes"
)

func main() {
	c := cmd.NewCommandArchiver("archiver")
	flagtypes.GLog(c.Flags())
	if err := c.Execute(); err != nil {
		os.Exit(1)
	}
}
