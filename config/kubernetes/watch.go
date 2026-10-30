package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-jimu/components/config"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// minWatchLifetime separates the API server ending a healthy watch, which is
// reopened at once, from a watch that fails right after opening, which is
// reported so config retries it with backoff.
const minWatchLifetime = time.Second

var errWatchClosed = errors.New("kubernetes configmap watch closed right after opening")

type watcher struct {
	k      *kube
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	watcher watch.Interface // nil until the next watch is opened
	opened  time.Time
}

func newWatcher(k *kube) (config.Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcher{k: k, ctx: ctx, cancel: cancel}
	if _, err := w.results(); err != nil {
		cancel()
		return nil, err
	}
	return w, nil
}

// results returns the result channel of the current watch, opening a new
// watch when there is none. Once Stop is called it returns context.Canceled.
func (w *watcher) results() (<-chan watch.Event, error) {
	w.mu.Lock()
	if w.watcher != nil {
		defer w.mu.Unlock()
		return w.watcher.ResultChan(), nil
	}
	w.mu.Unlock()

	next, err := w.k.client.CoreV1().ConfigMaps(w.k.opts.Namespace).Watch(w.ctx, metav1.ListOptions{
		LabelSelector: w.k.opts.LabelSelector,
		FieldSelector: w.k.opts.FieldSelector,
	})
	if err != nil {
		if w.ctx.Err() != nil {
			return nil, context.Canceled
		}
		return nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx.Err() != nil {
		next.Stop()
		return nil, context.Canceled
	}
	w.watcher = next
	w.opened = time.Now()
	return next.ResultChan(), nil
}

// drop stops the current watch so the next call to results opens a new one,
// and reports how long the dropped watch was open.
func (w *watcher) drop() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.watcher == nil {
		return 0
	}
	w.watcher.Stop()
	w.watcher = nil
	return time.Since(w.opened)
}

// Next returns the key values of the next changed ConfigMap. The API server
// ends watches from time to time; Next opens a new one and keeps waiting.
// After Stop, Next returns context.Canceled.
func (w *watcher) Next() ([]*config.KeyValue, error) {
	for {
		results, err := w.results()
		if err != nil {
			return nil, err
		}
		select {
		case <-w.ctx.Done():
			return nil, context.Canceled
		case event, ok := <-results:
			if !ok || event.Object == nil {
				if w.ctx.Err() != nil {
					// Stop closed the watch.
					return nil, context.Canceled
				}
				if w.drop() < minWatchLifetime {
					return nil, errWatchClosed
				}
				continue
			}
			switch event.Type {
			case watch.Error:
				w.drop()
				return nil, fmt.Errorf("kubernetes configmap watch: %w", apierrors.FromObject(event.Object))
			case watch.Deleted:
				return nil, fmt.Errorf("kubernetes configmap delete %s", configMapName(event.Object))
			default:
			}
			cm, isConfigMap := event.Object.(*v1.ConfigMap)
			if !isConfigMap {
				return nil, fmt.Errorf("kubernetes object %T is not a ConfigMap", event.Object)
			}
			return w.k.configMap(*cm), nil
		}
	}
}

func configMapName(obj any) string {
	if cm, ok := obj.(*v1.ConfigMap); ok {
		return cm.Name
	}
	return fmt.Sprintf("%T", obj)
}

func (w *watcher) Stop() error {
	w.cancel()
	w.drop()
	return nil
}
