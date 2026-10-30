package apollo

import (
	"context"
	"testing"
	"time"

	"github.com/apolloconfig/agollo/v5/agcache"
	"github.com/apolloconfig/agollo/v5/agcache/memory"
	"github.com/apolloconfig/agollo/v5/storage"
	"github.com/go-jimu/components/config"
	"github.com/go-jimu/components/encoding"
)

func Test_onChange(t *testing.T) {
	s := map[string]struct {
		Name string `yaml:"name"`
	}{
		"app": {
			Name: "new",
		},
	}
	codec := encoding.GetCodec(yaml)
	val, _ := codec.Marshal(s)
	cache := (&memory.DefaultCacheFactory{}).Create()
	_ = cache.Set("name", "new", 0)
	c := customChangeListener{cacheOf: func(string) agcache.CacheInterface { return cache }}
	tests := []struct {
		name      string
		namespace string
		changes   map[string]*storage.ConfigChange
		kvs       []*config.KeyValue
	}{
		{
			"test yaml onChange",
			"app.yaml",
			map[string]*storage.ConfigChange{
				"name": {
					OldValue:   "old",
					NewValue:   "new",
					ChangeType: storage.MODIFIED,
				},
			},
			[]*config.KeyValue{
				{
					Key:    "app.yaml",
					Value:  val,
					Format: yaml,
				},
			},
		},
		{
			"test json onChange",
			"app.json",
			map[string]*storage.ConfigChange{
				"content": {
					OldValue:   `{"name":"old"}`,
					NewValue:   `{"name":"new"}`,
					ChangeType: storage.MODIFIED,
				},
			},
			[]*config.KeyValue{
				{
					Key:    "app.json",
					Value:  []byte(`{"name":"new"}`),
					Format: json,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kvs := c.onChange(tt.namespace, tt.changes)
			if len(kvs) != len(tt.kvs) {
				t.Errorf("len(kvs) = %v, want %v", len(kvs), len(tt.kvs))
			}
			for i := range kvs {
				if kvs[i].Format != tt.kvs[i].Format || kvs[i].Key != tt.kvs[i].Key || string(kvs[i].Value) != string(tt.kvs[i].Value) {
					t.Errorf("got %v, want %v", kvs[i], tt.kvs[i])
				}
			}
		})
	}
}

// Intent: a change to a properties namespace reports only the changed keys,
// while config replaces the namespace with the reported value. The unchanged
// keys must survive the reload.
func TestOnChangeKeepsUnchangedPropertiesKeys(t *testing.T) {
	cache := (&memory.DefaultCacheFactory{}).Create()
	_ = cache.Set("a", "1", 0)
	_ = cache.Set("b", "2", 0)
	listener := customChangeListener{cacheOf: func(string) agcache.CacheInterface { return cache }}
	loaded, err := namespaceKeyValue("application", cache)
	if err != nil {
		t.Fatalf("namespaceKeyValue: %v", err)
	}

	// agollo updates the cache, then reports only the changed key.
	_ = cache.Set("a", "3", 0)
	changed := listener.onChange("application", map[string]*storage.ConfigChange{
		"a": {OldValue: "1", NewValue: "3", ChangeType: storage.MODIFIED},
	})

	source := &changeSource{loaded: []*config.KeyValue{loaded}, next: make(chan []*config.KeyValue, 1)}
	cfg := config.New(config.WithSource(source))
	if err := cfg.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer cfg.Close()
	source.next <- changed

	var got struct {
		Application map[string]string `json:"application"`
	}
	deadline := time.Now().Add(time.Second)
	for {
		got.Application = nil
		if err := cfg.Scan(&got); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if got.Application["a"] == "3" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("application = %v, want a=3", got.Application)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got.Application["b"] != "2" {
		t.Fatalf("application = %v, want b=2 kept", got.Application)
	}
}

type changeSource struct {
	loaded []*config.KeyValue
	next   chan []*config.KeyValue
}

func (s *changeSource) Load() ([]*config.KeyValue, error) { return s.loaded, nil }
func (s *changeSource) Watch() (config.Watcher, error) {
	return &changeWatcher{source: s, done: make(chan struct{})}, nil
}

type changeWatcher struct {
	source *changeSource
	done   chan struct{}
}

func (w *changeWatcher) Next() ([]*config.KeyValue, error) {
	select {
	case kvs := <-w.source.next:
		return kvs, nil
	case <-w.done:
		return nil, context.Canceled
	}
}

func (w *changeWatcher) Stop() error {
	close(w.done)
	return nil
}
