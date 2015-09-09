package etcdcmd

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/client"
	etcdstorage "k8s.io/kubernetes/pkg/storage/etcd"

	"github.com/openshift/origin/pkg/cmd/flagtypes"
	"github.com/openshift/origin/pkg/cmd/util"
)

const ConfigSyntax = " --etcd=<addr>"

type Config struct {
	EtcdAddr flagtypes.Addr
	KeyFile  string
	CertFile string
	CAFile   string
}

func NewConfig() *Config {
	return &Config{
		EtcdAddr: flagtypes.Addr{Value: "localhost:4001", DefaultScheme: "https", DefaultPort: 4001, AllowPrefix: false}.Default(),
	}
}

func (cfg *Config) Bind(flag *pflag.FlagSet) {
	flag.Var(&cfg.EtcdAddr, "etcd", "The address etcd can be reached on (host, host:port, or URL).")
	flag.StringVar(&cfg.KeyFile, "keyfile", "", "Path to a key file for the server.")
	flag.StringVar(&cfg.CertFile, "certfile", "", "Path to a cert file for the server.")
	flag.StringVar(&cfg.CAFile, "cafile", "", "Path to a CA file for the server.")
}

func (cfg *Config) bindEnv() {
	if value, ok := util.GetEnv("ETCD_ADDR"); ok {
		cfg.EtcdAddr.Set(value)
	}
}

func (cfg *Config) Client(check bool) (*etcd.Client, error) {
	cfg.bindEnv()

	client, err := etcdClient(cfg)
	if err != nil {
		return nil, err
	}

	if check {
		for i := 0; ; i += 1 {
			_, err := client.Get("/", false, false)
			if err == nil || etcdstorage.IsEtcdNotFound(err) {
				break
			}
			if i > 100 {
				return nil, fmt.Errorf("Could not reach etcd at %q: %v", cfg.EtcdAddr.URL, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	return client, nil
}

// etcdClient creates an etcd client based on the provided config.
func etcdClient(cfg *Config) (*etcd.Client, error) {
	// etcd does a poor job of setting up the transport - use the Kube client stack
	transport, err := client.TransportFor(&client.Config{
		TLSClientConfig: client.TLSClientConfig{
			CertFile: cfg.CertFile,
			KeyFile:  cfg.KeyFile,
			CAFile:   cfg.CAFile,
		},
		WrapTransport: defaultEtcdClientTransport,
	})
	if err != nil {
		return nil, err
	}

	etcdClient := etcd.NewClient([]string{cfg.EtcdAddr.URL.String()})
	etcdClient.SetTransport(transport.(*http.Transport))
	return etcdClient, nil
}

// defaultEtcdClientTransport sets defaults for an etcd Transport that are suitable
// for use by infrastructure components.
func defaultEtcdClientTransport(rt http.RoundTripper) http.RoundTripper {
	transport := rt.(*http.Transport)
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
		// Lower the keep alive for connections.
		KeepAlive: 1 * time.Second,
	}
	transport.Dial = dialer.Dial
	// Because watches are very bursty, defends against long delays
	// in watch reconnections.
	transport.MaxIdleConnsPerHost = 10
	return transport
}
