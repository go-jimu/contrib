package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// fakeKV serves Get from a fixed value.
type fakeKV struct {
	clientv3.KV
	value string
}

func (f *fakeKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("/app/config.yaml"), Value: []byte(f.value)}}}, nil
}

// fakeWatcher hands out watch channels the test drives, and records the
// context of each watch.
type fakeWatcher struct {
	clientv3.Watcher
	opened chan chan clientv3.WatchResponse
	ctxs   chan context.Context
}

func (f *fakeWatcher) Watch(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	ch := make(chan clientv3.WatchResponse, 1)
	f.ctxs <- ctx
	f.opened <- ch
	return ch
}

func newFakeSource(value string) (*source, *fakeWatcher) {
	w := &fakeWatcher{opened: make(chan chan clientv3.WatchResponse, 4), ctxs: make(chan context.Context, 4)}
	client := &clientv3.Client{KV: &fakeKV{value: value}, Watcher: w}
	return &source{client: client, options: &options{ctx: context.Background(), path: "/app/config.yaml"}}, w
}

// Intent: a closed watch channel is reported once, then the watch reopens and
// Next reloads, instead of returning the same values forever without waiting.
func TestWatcherReopensClosedWatch(t *testing.T) {
	s, fake := newFakeSource("port: 80")
	w := newWatcher(s)
	defer w.Stop()
	<-fake.ctxs
	close(<-fake.opened)

	if _, err := w.Next(); !errors.Is(err, errWatchClosed) {
		t.Fatalf("Next error = %v, want errWatchClosed", err)
	}
	kvs, err := w.Next()
	if err != nil || len(kvs) != 1 || string(kvs[0].Value) != "port: 80" {
		t.Fatalf("Next = %v, %v; want a reload after reopening", kvs, err)
	}
	select {
	case <-fake.opened:
	default:
		t.Fatal("Next did not reopen the watch")
	}
}

// Intent: Stop cancels the context of the etcd watch stream, and Next returns
// context.Canceled, which ends the config watch.
func TestWatcherStopCancelsWatch(t *testing.T) {
	s, fake := newFakeSource("port: 80")
	w := newWatcher(s)
	watchCtx := <-fake.ctxs

	result := make(chan error, 1)
	go func() {
		_, err := w.Next()
		result <- err
	}()
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case <-watchCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the watch context")
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Next error = %v, want context.Canceled", err)
	}
}
