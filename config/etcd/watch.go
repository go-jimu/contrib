package etcd

import (
	"context"
	"errors"

	"github.com/go-jimu/components/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var errWatchClosed = errors.New("etcd watch channel closed")

type watcher struct {
	source *source
	ch     clientv3.WatchChan // nil after the watch closed, until Next reopens it

	ctx    context.Context
	cancel context.CancelFunc
}

func newWatcher(s *source) *watcher {
	// The watch runs on a context of its own, so Stop cancels the etcd watch
	// stream, while the source context still bounds it.
	ctx, cancel := context.WithCancel(s.options.ctx)
	w := &watcher{
		source: s,
		ctx:    ctx,
		cancel: cancel,
	}
	w.ch = w.open()
	return w
}

func (w *watcher) open() clientv3.WatchChan {
	var opts []clientv3.OpOption
	if w.source.options.prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	return w.source.client.Watch(w.ctx, w.source.options.path, opts...)
}

// Next returns the source's key values after each change. etcd closes the
// watch channel when it cancels the watch, for example after a compaction;
// Next reports that so config retries with backoff, then reopens the watch
// and reloads, since changes made while it was closed were not observed.
func (w *watcher) Next() ([]*config.KeyValue, error) {
	if w.ch == nil {
		if w.ctx.Err() != nil {
			return nil, context.Canceled
		}
		w.ch = w.open()
		return w.source.Load()
	}
	select {
	case resp, ok := <-w.ch:
		if w.ctx.Err() != nil {
			return nil, context.Canceled
		}
		if !ok {
			w.ch = nil
			return nil, errWatchClosed
		}
		if err := resp.Err(); err != nil {
			return nil, err
		}
		return w.source.Load()
	case <-w.ctx.Done():
		return nil, context.Canceled
	}
}

func (w *watcher) Stop() error {
	w.cancel()
	return nil
}
