package storageconsul

import (
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

func init() {
	caddy.RegisterModule(ConsulStorage{})
}

func (ConsulStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.consul",
		New: func() caddy.Module {
			return New()
		},
	}
}

// Provision is called by Caddy to prepare the module
func (cs *ConsulStorage) Provision(ctx caddy.Context) error {
	cs.logger = ctx.Logger(cs).Sugar()

	// override default values from ENV
	if aesKey := os.Getenv(EnvNameAESKey); aesKey != "" {
		cs.AESKey = []byte(aesKey)
	}

	if prefix := os.Getenv(EnvNamePrefix); prefix != "" {
		cs.Prefix = prefix
	}

	if valueprefix := os.Getenv(EnvValuePrefix); valueprefix != "" {
		cs.ValuePrefix = valueprefix
	}

	err := cs.createConsulClient()
	if err != nil {
		return err
	}

	peers, _ := cs.ConsulClient.Status().Peers()
	cs.logger.Infof("TLS storage is using Consul at %v", peers)

	return nil
}

func (cs *ConsulStorage) CertMagicStorage() (certmagic.Storage, error) {
	return cs, nil
}

// UnmarshalCaddyfile parses plugin settings from Caddyfile
//
//	storage consul {
//	    address      "127.0.0.1:8500"
//	    token        "consul-access-token"
//	    timeout      10
//	    prefix       "caddytls"
//	    value_prefix "myprefix"
//	    aes_key      "consultls-1234567890-caddytls-32"
//	    tls_enabled  "false"
//	    tls_insecure "true"
//	}
func (cs *ConsulStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		key := d.Val()
		var value string

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "address":
			if value != "" {
				parsedAddress, err := caddy.ParseNetworkAddress(value)
				if err == nil {
					cs.Address = parsedAddress.JoinHostPort(0)
				}
			}
		case "token":
			if value != "" {
				cs.Token = value
			}
		case "timeout":
			if value != "" {
				timeParse, err := strconv.Atoi(value)
				if err == nil {
					cs.Timeout = timeParse
				}
			}
		case "prefix":
			if value != "" {
				cs.Prefix = value
			}
		case "value_prefix":
			if value != "" {
				cs.ValuePrefix = value
			}
		case "aes_key":
			if value != "" {
				cs.AESKey = []byte(value)
			}
		case "tls_enabled":
			if value != "" {
				tlsParse, err := strconv.ParseBool(value)
				if err == nil {
					cs.TlsEnabled = tlsParse
				}
			}
		case "tls_insecure":
			if value != "" {
				tlsInsecureParse, err := strconv.ParseBool(value)
				if err == nil {
					cs.TlsInsecure = tlsInsecureParse
				}
			}
		case "url":
			if value != "" {
				consulurl, err := url.Parse(value)
				if err == nil {
					if consulurl.Scheme == "https" {
						cs.TlsEnabled = true
					} else {
						cs.TlsEnabled = false
					}
					if consulurl.Host != "" {
						cs.Address = caddy.JoinNetworkAddress("", consulurl.Hostname(), consulurl.Port())
					}
					password, isset := consulurl.User.Password()
					if isset {
						cs.Token = password
					}
					cs.Prefix = strings.TrimPrefix(consulurl.Path, "/")
				}
			}
		}
	}
	return nil
}
