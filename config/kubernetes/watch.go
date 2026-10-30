package kubernetes

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-jimu/components/config"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

type watcher struct {
	k      *kube
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	watcher watch.Interface
}

func newWatcher(k *kube) (config.Watcher, error) {
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcher{k: k, ctx: ctx, cancel: cancel}
	if _, err := w.rewatch(); err != nil {
		cancel()
		return nil, err
	}
	return w, nil
}

// rewatch opens a new ConfigMap watch and returns its result channel. Once
// Stop is called it returns context.Canceled.
func (w *watcher) rewatch() (<-chan watch.Event, error) {
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
	return next.ResultChan(), nil
}

func (w *watcher) resultChan() <-chan watch.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.watcher.ResultChan()
}

// Next returns the key values of the next changed ConfigMap. The API server
// closes watches from time to time; Next opens a new one and keeps waiting.
// After Stop, Next returns context.Canceled.
func (w *watcher) Next() ([]*config.KeyValue, error) {
	results := w.resultChan()
	for {
		select {
		case <-w.ctx.Done():
			return nil, context.Canceled
		case event, ok := <-results:
			if !ok || event.Object == nil {
				var err error
				if results, err = w.rewatch(); err != nil {
					return nil, err
				}
				continue
			}
			cm, isConfigMap := event.Object.(*v1.ConfigMap)
			if !isConfigMap {
				return nil, fmt.Errorf("kubernetes Object not ConfigMap")
			}
			if event.Type == watch.Deleted {
				return nil, fmt.Errorf("kubernetes configmap delete %s", cm.Name)
			}
			return w.k.configMap(*cm), nil
		}
	}
}

func (w *watcher) Stop() error {
	w.cancel()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.watcher.Stop()
	return nil
}
