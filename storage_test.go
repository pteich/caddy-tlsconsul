// +build consul

package storageconsul

import (
	consul "github.com/hashicorp/consul/api"
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/fs"
	"os"
	"path"
	"testing"
	"time"
)

var consulClient *consul.Client

const TestPrefix = "consultlstest"

// these tests needs a running Consul server
func setupConsulEnv(t *testing.T) (*ConsulStorage, context.Context) {

	os.Setenv(consul.HTTPTokenEnvName, "2f9e03f8-714b-5e4d-65ea-c983d6b172c4")

	cs := New()
	cs.createConsulClient()
	cs.Prefix = TestPrefix
	cs.logger = zap.NewExample().Sugar()

	_, err := cs.ConsulClient.KV().DeleteTree(TestPrefix, nil)
	assert.NoError(t, err)

	ctx := context.Background()

	return cs, ctx
}

func TestConsulStorage_Store(t *testing.T) {
	cs, ctx := setupConsulEnv(t)

	err := cs.Store(ctx, path.Join("acme", "example.com", "sites", "example.com", "example.com.crt"), []byte("crt data"))
	assert.NoError(t, err)
}

func TestConsulStorage_Exists(t *testing.T) {
	cs, ctx := setupConsulEnv(t)

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")

	err := cs.Store(ctx, key, []byte("crt data"))
	assert.NoError(t, err)

	exists := cs.Exists(ctx, key)
	assert.True(t, exists)
}

func TestConsulStorage_Load(t *testing.T) {
	cs, ctx := setupConsulEnv(t)

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")
	content := []byte("crt data")

	err := cs.Store(ctx, key, content)
	assert.NoError(t, err)

	contentLoded, err := cs.Load(ctx, key)
	assert.NoError(t, err)

	assert.Equal(t, content, contentLoded)
}

func TestConsulStorage_Delete(t *testing.T) {
	cs, ctx := setupConsulEnv(t)

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")
	content := []byte("crt data")

	err := cs.Store(ctx, key, content)
	assert.NoError(t, err)

	err = cs.Delete(ctx, key)
	assert.NoError(t, err)

	exists := cs.Exists(ctx, key)
	assert.False(t, exists)

	contentLoaded, err := cs.Load(ctx, key)
	assert.Nil(t, contentLoaded)

	assert.True(t, errors.Is(err, fs.ErrNotExist))
}

func TestConsulStorage_Stat(t *testing.T) {
	cs, ctx := setupConsulEnv(t)

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")
	content := []byte("crt data")

	err := cs.Store(ctx, key, content)
	assert.NoError(t, err)

	info, err := cs.Stat(ctx, key)
	assert.NoError(t, err)

	assert.Equal(t, key, info.Key)
}

func TestConsulStorage_List(t *testing.T) {
	cs, ctx := setupConsulEnv(t)

	err := cs.Store(ctx, path.Join("acme", "example.com", "sites", "example.com", "example.com.crt"), []byte("crt"))
	assert.NoError(t, err)
	err = cs.Store(ctx, path.Join("acme", "example.com", "sites", "example.com", "example.com.key"), []byte("key"))
	assert.NoError(t, err)
	err = cs.Store(ctx, path.Join("acme", "example.com", "sites", "example.com", "example.com.json"), []byte("meta"))
	assert.NoError(t, err)

	keys, err := cs.List(ctx, path.Join("acme", "example.com", "sites", "example.com"), true)
	assert.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, path.Join("acme", "example.com", "sites", "example.com", "example.com.crt"))
}

func TestConsulStorage_ListNonRecursive(t *testing.T) {
	cs, ctx := setupConsulEnv(t)

	err := cs.Store(ctx, path.Join("acme", "example.com", "sites", "example.com", "example.com.crt"), []byte("crt"))
	assert.NoError(t, err)
	err = cs.Store(ctx, path.Join("acme", "example.com", "sites", "example.com", "example.com.key"), []byte("key"))
	assert.NoError(t, err)
	err = cs.Store(ctx, path.Join("acme", "example.com", "sites", "example.com", "example.com.json"), []byte("meta"))
	assert.NoError(t, err)

	keys, err := cs.List(ctx, path.Join("acme", "example.com", "sites"), false)
	assert.NoError(t, err)

	assert.Len(t, keys, 1)
	assert.Contains(t, keys, path.Join("acme", "example.com", "sites", "example.com"))
}

func TestConsulStorage_LockUnlock(t *testing.T) {
	cs, ctx := setupConsulEnv(t)
	lockKey := path.Join("acme", "example.com", "sites", "example.com", "lock")

	err := cs.Lock(ctx, lockKey)
	assert.NoError(t, err)

	err = cs.Unlock(ctx, lockKey)
	assert.NoError(t, err)
}

func TestConsulStorage_TwoLocks(t *testing.T) {
	cs, ctx := setupConsulEnv(t)
	cs2, ctx2 := setupConsulEnv(t)
	lockKey := path.Join("acme", "example.com", "sites", "example.com", "lock")

	err := cs.Lock(ctx, lockKey)
	assert.NoError(t, err)

	go time.AfterFunc(5*time.Second, func() {
		err = cs.Unlock(ctx, lockKey)
		assert.NoError(t, err)
	})

	err = cs2.Lock(ctx2, lockKey)
	assert.NoError(t, err)

	err = cs2.Unlock(ctx2, lockKey)
	assert.NoError(t, err)
}
