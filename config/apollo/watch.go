package apollo

import (
	"context"
	"log/slog"
	"strings"

	"github.com/apolloconfig/agollo/v5/agcache"
	"github.com/apolloconfig/agollo/v5/storage"
	"github.com/go-jimu/components/config"
	"github.com/go-jimu/components/sloghelper"
)

type watcher struct {
	out <-chan []*config.KeyValue

	ctx      context.Context
	cancelFn func()
}

type customChangeListener struct {
	in chan<- []*config.KeyValue
	// cacheOf returns the cache of a namespace, which agollo has already
	// updated when it reports a change.
	cacheOf func(namespace string) agcache.CacheInterface
}

func (c *customChangeListener) onChange(namespace string, changes map[string]*storage.ConfigChange) []*config.KeyValue {
	kv := make([]*config.KeyValue, 0, 2)
	if strings.Contains(namespace, ".") && !strings.HasSuffix(namespace, "."+properties) &&
		(format(namespace) == yaml || format(namespace) == yml || format(namespace) == json) {
		if value, ok := changes["content"]; ok {
			kv = append(kv, &config.KeyValue{
				Key:    namespace,
				Value:  []byte(value.NewValue.(string)),
				Format: format(namespace),
			})

			return kv
		}
	}

	// changes holds only the changed keys; rebuild the namespace from its cache
	// so the unchanged keys stay in config.
	next, err := namespaceKeyValue(namespace, c.cacheOf(namespace))
	if err != nil {
		slog.Warn("apollo could not handle namespace", slog.String("namespace", namespace), sloghelper.Error(err))
		return nil
	}
	return append(kv, next)
}

func (c *customChangeListener) OnChange(changeEvent *storage.ChangeEvent) {
	change := c.onChange(changeEvent.Namespace, changeEvent.Changes)
	if len(change) == 0 {
		return
	}

	c.in <- change
}

func (c *customChangeListener) OnNewestChange(_ *storage.FullChangeEvent) {}

func newWatcher(a *apollo) (config.Watcher, error) {
	changeCh := make(chan []*config.KeyValue)
	listener := &customChangeListener{in: changeCh, cacheOf: a.client.GetConfigCache}
	a.client.AddChangeListener(listener)

	ctx, cancel := context.WithCancel(context.Background())
	return &watcher{
		out: changeCh,

		ctx: ctx,
		cancelFn: func() {
			a.client.RemoveChangeListener(listener)
			cancel()
		},
	}, nil
}

// Next will be blocked until the Stop method is called
func (w *watcher) Next() ([]*config.KeyValue, error) {
	select {
	case kv := <-w.out:
		return kv, nil
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	}
}

func (w *watcher) Stop() error {
	if w.cancelFn != nil {
		w.cancelFn()
	}
	return nil
}
