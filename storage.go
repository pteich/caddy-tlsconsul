package storageconsul

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	consul "github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

var (
	ErrLockNotFound = errors.New("lock not found")
)

// ConsulStorage allows to store certificates and other TLS resources
// in a shared cluster environment using Consul's key/value-store.
// It uses distributed locks to ensure consistency.
type ConsulStorage struct {
	certmagic.Storage
	ConsulClient *consul.Client
	logger       *zap.SugaredLogger
	muLocks      *sync.RWMutex
	locks        map[string]*consul.Lock

	Address     string `json:"address"`
	Token       string `json:"token"`
	Timeout     int    `json:"timeout"`
	Prefix      string `json:"prefix"`
	ValuePrefix string `json:"value_prefix"`
	AESKey      []byte `json:"aes_key"`
	TlsEnabled  bool   `json:"tls_enabled"`
	TlsInsecure bool   `json:"tls_insecure"`
}

// New connects to Consul and returns a ConsulStorage
func New() *ConsulStorage {
	// create ConsulStorage and pre-set values
	s := ConsulStorage{
		locks:       make(map[string]*consul.Lock),
		AESKey:      []byte(DefaultAESKey),
		ValuePrefix: DefaultValuePrefix,
		Prefix:      DefaultPrefix,
		Timeout:     DefaultTimeout,
		muLocks:     &sync.RWMutex{},
	}

	return &s
}

func (cs *ConsulStorage) prefixKey(key string) string {
	return path.Join(cs.Prefix, key)
}

// Lock acquires a distributed lock for the given key or blocks until it gets one
func (cs *ConsulStorage) Lock(ctx context.Context, key string) error {
	cs.logger.Debugf("trying lock for %s", key)

	if _, isLocked := cs.GetLock(key); isLocked {
		return nil
	}

	// prepare the distributed lock
	cs.logger.Infof("creating Consul lock for %s", key)
	lock, err := cs.ConsulClient.LockOpts(&consul.LockOptions{
		Key:          cs.prefixKey(key),
		LockWaitTime: time.Duration(cs.Timeout) * time.Second,
		LockTryOnce:  true,
	})
	if err != nil {
		return fmt.Errorf("could not create lock for %s: %w", cs.prefixKey(key), err)
	}

	// acquire the lock and return a channel that is closed upon lost
	lockActive, err := lock.Lock(ctx.Done())
	if err != nil {
		return fmt.Errorf("unable to lock %s: %w", cs.prefixKey(key), err)
	}

	// auto-unlock and clean list of locks in case of lost
	go func() {
		<-lockActive
		err := cs.Unlock(ctx, key)
		if err != nil && !errors.Is(err, ErrLockNotFound) {
			cs.logger.Errorf("failed to release lock: %s", err)
		}
	}()

	// save the lock
	cs.muLocks.Lock()
	cs.locks[key] = lock
	cs.muLocks.Unlock()

	return nil
}

func (cs *ConsulStorage) GetLock(key string) (*consul.Lock, bool) {
	cs.muLocks.RLock()
	defer cs.muLocks.RUnlock()

	// if we already hold the lock, return early
	if lock, exists := cs.locks[key]; exists {
		return lock, true
	}

	return nil, false
}

// Unlock releases a specific lock
func (cs *ConsulStorage) Unlock(_ context.Context, key string) error {
	// check if we own it and unlock
	lock, exists := cs.GetLock(key)
	if !exists {
		return fmt.Errorf("lock %s not found: %w", cs.prefixKey(key), ErrLockNotFound)
	}

	err := lock.Unlock()
	if err != nil {
		return fmt.Errorf("unable to unlock %s: %w", cs.prefixKey(key), err)
	}

	cs.muLocks.Lock()
	delete(cs.locks, key)
	cs.muLocks.Unlock()

	return nil
}

// Store saves encrypted data value for a key in Consul KV
func (cs *ConsulStorage) Store(ctx context.Context, key string, value []byte) error {
	kv := &consul.KVPair{Key: cs.prefixKey(key)}

	// prepare the stored data
	consulData := &StorageData{
		Value:    value,
		Modified: time.Now(),
	}

	encryptedValue, err := cs.EncryptStorageData(consulData)
	if err != nil {
		return fmt.Errorf("unable to encode data for %s: %w", cs.prefixKey(key), err)
	}

	kv.Value = encryptedValue

	opts := consul.WriteOptions{}
	if _, err = cs.ConsulClient.KV().Put(kv, opts.WithContext(ctx)); err != nil {
		return fmt.Errorf("unable to store data for %s: %w", cs.prefixKey(key), err)
	}

	return nil
}

// Load retrieves the value for a key from Consul KV
func (cs *ConsulStorage) Load(ctx context.Context, key string) ([]byte, error) {
	cs.logger.Debugf("loading data from Consul for %s", key)

	kv, _, err := cs.ConsulClient.KV().Get(cs.prefixKey(key), ConsulQueryDefaults(ctx))
	if err != nil {
		return nil, err
	}

	if kv == nil {
		return nil, fs.ErrNotExist
	}

	contents, err := cs.DecryptStorageData(kv.Value)
	if err != nil {
		return nil, fmt.Errorf("unable to decrypt data for %s: %w", cs.prefixKey(key), err)
	}

	return contents.Value, nil
}

// Delete a key from Consul KV
func (cs *ConsulStorage) Delete(ctx context.Context, key string) error {
	cs.logger.Infof("deleting key %s from Consul", key)

	// first obtain existing keypair
	kv, _, err := cs.ConsulClient.KV().Get(cs.prefixKey(key), ConsulQueryDefaults(ctx))
	if err != nil {
		return fmt.Errorf("%s: %w", err, fs.ErrNotExist)
	}

	if kv == nil {
		return fs.ErrNotExist
	}

	// now do a Check-And-Set operation to verify we really deleted the key
	if success, _, err := cs.ConsulClient.KV().DeleteCAS(kv, nil); err != nil {
		return fmt.Errorf("unable to delete data for %s: %w", cs.prefixKey(key), err)
	} else if !success {
		return fmt.Errorf("failed to lock data delete for %s", cs.prefixKey(key))
	}

	return nil
}

// Exists checks if a key exists
func (cs *ConsulStorage) Exists(ctx context.Context, key string) bool {
	kv, _, err := cs.ConsulClient.KV().Get(cs.prefixKey(key), ConsulQueryDefaults(ctx))
	if kv != nil && err == nil {
		return true
	}
	return false
}

// List returns a list with all keys under a given prefix
func (cs *ConsulStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	var keysFound []string

	// get a list of all keys at prefix
	keys, _, err := cs.ConsulClient.KV().Keys(cs.prefixKey(prefix), "", ConsulQueryDefaults(ctx))
	if err != nil {
		return keysFound, err
	}

	if len(keys) == 0 {
		return keysFound, fs.ErrNotExist
	}

	// remove default prefix from keys
	for _, key := range keys {
		if strings.HasPrefix(key, cs.prefixKey(prefix)) {
			key = strings.TrimPrefix(key, cs.Prefix+"/")
			keysFound = append(keysFound, key)
		}
	}

	// if recursive wanted, just return all keys
	if recursive {
		return keysFound, nil
	}

	// for non-recursive split path and look for unique keys just under given prefix
	keysMap := make(map[string]bool)
	for _, key := range keysFound {
		dir := strings.Split(strings.TrimPrefix(key, prefix+"/"), "/")
		keysMap[dir[0]] = true
	}

	keysFound = make([]string, 0)
	for key := range keysMap {
		keysFound = append(keysFound, path.Join(prefix, key))
	}

	return keysFound, nil
}

// Stat returns statistic data of a key
func (cs *ConsulStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	kv, _, err := cs.ConsulClient.KV().Get(cs.prefixKey(key), ConsulQueryDefaults(ctx))
	if err != nil {
		return certmagic.KeyInfo{}, fmt.Errorf("unable to obtain data for %s: %w", cs.prefixKey(key), fs.ErrNotExist)
	}
	if kv == nil {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	contents, err := cs.DecryptStorageData(kv.Value)
	if err != nil {
		return certmagic.KeyInfo{}, fmt.Errorf("unable to decrypt data for %s: %w", cs.prefixKey(key), err)
	}

	return certmagic.KeyInfo{
		Key:        key,
		Modified:   contents.Modified,
		Size:       int64(len(contents.Value)),
		IsTerminal: false,
	}, nil
}

func (cs *ConsulStorage) createConsulClient() error {
	// get the default config
	consulCfg := consul.DefaultConfig()
	if cs.Address != "" {
		consulCfg.Address = cs.Address
	} else {
		cs.Address = consulCfg.Address
	}
	if cs.Token != "" {
		consulCfg.Token = cs.Token
	}
	if cs.TlsEnabled {
		consulCfg.Scheme = "https"
	}
	consulCfg.TLSConfig.InsecureSkipVerify = cs.TlsInsecure

	// set a dial context to prevent default keepalive
	consulCfg.Transport.DialContext = (&net.Dialer{
		Timeout:   time.Duration(cs.Timeout) * time.Second,
		KeepAlive: time.Duration(cs.Timeout) * time.Second,
	}).DialContext

	// create the Consul API client
	consulClient, err := consul.NewClient(consulCfg)
	if err != nil {
		return fmt.Errorf("unable to create Consul client: %w", err)
	}

	if _, err := consulClient.Agent().NodeName(); err != nil {
		return fmt.Errorf("unable to ping Consul: %w", err)
	}

	cs.ConsulClient = consulClient
	return nil
}

func ConsulQueryDefaults(ctx context.Context) *consul.QueryOptions {
	opts := &consul.QueryOptions{
		UseCache:          false,
		RequireConsistent: false,
	}
	return opts.WithContext(ctx)
}

// Interface guards
var (
	_ caddy.StorageConverter = (*ConsulStorage)(nil)
	_ caddyfile.Unmarshaler  = (*ConsulStorage)(nil)
)
