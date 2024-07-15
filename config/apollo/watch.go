package apollo

import (
	"context"
	"log/slog"
	"strings"

	"github.com/apolloconfig/agollo/v4/storage"
	"github.com/go-jimu/components/config"
	"github.com/go-jimu/components/encoding"
	"github.com/go-jimu/components/sloghelper"
)

type watcher struct {
	out <-chan []*config.KeyValue

	ctx      context.Context
	cancelFn func()
}

type customChangeListener struct {
	in     chan<- []*config.KeyValue
	apollo *apollo
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

	next := make(map[string]interface{})

	for key, change := range changes {
		resolve(genKey(namespace, key), change.NewValue, next)
	}

	f := format(namespace)
	codec := encoding.GetCodec(f)
	val, err := codec.Marshal(next)
	if err != nil {
		slog.Warn("apollo could not handle namespace", slog.String("namespace", namespace), sloghelper.Error(err))
		return nil
	}
	kv = append(kv, &config.KeyValue{
		Key:    namespace,
		Value:  val,
		Format: f,
	})

	return kv
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
	listener := &customChangeListener{in: changeCh, apollo: a}
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
