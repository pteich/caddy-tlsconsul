# Caddy 2 cluster / Certmagic TLS cluster support for Consul K/V

[Consul K/V](https://github.com/hashicorp/consul) Storage for [Caddy](https://github.com/caddyserver/caddy) TLS data. 

This cluster plugin enables Caddy 2 to store TLS data like keys and certificates in Consul's K/V store so you don't have to rely on a shared filesystem.
This allows you to use Caddy 2 in distributed environment and use a centralized storage for auto-generated certificates that is
shared between all Caddy instances. 

With this plugin it is possible to use multiple Caddy instances with the same HTTPS domain for instance with DNS round-robin.
All data that is saved in the KV store is encrypted using AES.

The version of this plugin in the master branch supports Caddy 2.0.0+ using CertMagic's [Storage Interface](https://pkg.go.dev/github.com/caddyserver/certmagic?tab=doc#Storage)

## Older versions

- For Caddy 0.10.x to 0.11.1 : use the `old_storage_interface` branch.
- For Caddy 1.x : use the `caddy1` branch.

## Docker Image
You can use my Docker image `pteich/caddy-tlsconsul` (https://hub.docker.com/r/pteich/caddy-tlsconsul) to get an image of Caddy including this Consul TLS plugin build in. It is based on the official caddy:2 image and will be build automatically on changes in this repo.

Available tags are `pteich/caddy-tlsconsul:latest` or semver compatible version tags like 1.4 or 1.4.1 referencing the versions of this plugin.


## Configuration

### Caddy configuration

ATTENTION: The name of the storage module in configurations has been changed to *consul* to align
with other storage modules.

You need to specify `consul` as the storage module in Caddy's configuration. This can be done in the config file of using the [admin API](https://caddyserver.com/docs/api).

JSON ([reference](https://caddyserver.com/docs/json/))
```
{
  "admin": {
    "listen": "0.0.0.0:2019"
  },
  "storage": {
    "module": "consul",
    "address": "localhost:8500",
    "prefix": "caddytls",
    "token": "consul-access-token",
    "aes_key": "consultls-1234567890-caddytls-32"
  }
}
```

Caddyfile ([reference](https://caddyserver.com/docs/caddyfile/options))
```
{
    storage consul {
           address      "127.0.0.1:8500"
           token        "consul-access-token"
           timeout      10
           prefix       "caddytls"
           value_prefix "myprefix"
           aes_key      "consultls-1234567890-caddytls-32"
           tls_enabled  "false"
           tls_insecure "true"
    }
}

:443 {
}
```

Several of the above properties can be condensed into a `url`:
```
    storage consul {
           url          "http://:consul-access-token@127.0.0.1:8500/caddytls"
           timeout      10
           value_prefix "myprefix"
           aes_key      "consultls-1234567890-caddytls-32"
           tls_insecure "true"
    }
```

### Consul configuration

Because this plugin uses the official Consul API client you can use all ENV variables like `CONSUL_HTTP_ADDR` or `CONSUL_HTTP_TOKEN`
to define your Consul address and token. For more information see https://github.com/hashicorp/consul/blob/master/api/api.go

Without any further configuration a running Consul on 127.0.0.1:8500 is assumed.

There are additional ENV variables for this plugin:

- `CADDY_CLUSTERING_CONSUL_AESKEY` defines your personal AES key to use when encrypting data. It needs to be 32 characters long.
- `CADDY_CLUSTERING_CONSUL_PREFIX` defines the prefix for the keys in KV store. Default is `caddytls`

### Consul ACL Policy

To access Consul you need a token with a valid ACL policy. Assuming you configured `caddytls` as your K/V path prefix you can use the following settings:
```
key_prefix "caddytls" {
	policy = "write"
}
session_prefix "" {
	policy = "write"
}
node_prefix "" {
	policy = "read"
}
agent_prefix "" {
	policy = "read"
}
```
