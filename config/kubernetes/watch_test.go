package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func newTestKube() *kube {
	return &kube{opts: options{Namespace: "default"}, client: fake.NewClientset()}
}

// Intent: Next reports ConfigMap changes as key values.
func TestWatcherNextReturnsChangedConfigMap(t *testing.T) {
	k := newTestKube()
	w, err := newWatcher(k)
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	defer w.Stop()

	_, err = k.client.CoreV1().ConfigMaps("default").Create(context.Background(), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "app"},
		Data:       map[string]string{"app.yaml": "port: 80"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	kvs, err := w.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(kvs) != 1 || kvs[0].Key != "default/app/app.yaml" || string(kvs[0].Value) != "port: 80" {
		t.Fatalf("kvs = %#v", kvs)
	}
}

// Intent: after Stop, Next returns context.Canceled, which config treats as
// the end of the watch, instead of reopening the closed watch forever.
func TestWatcherNextReturnsCanceledAfterStop(t *testing.T) {
	w, err := newWatcher(newTestKube())
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, nextErr := w.Next()
		result <- nextErr
	}()
	time.Sleep(20 * time.Millisecond)
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Next error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Next did not return after Stop")
	}
}

// newControlledKube returns a kube whose ConfigMap watches are fake watchers
// the test drives, and a channel that yields each watch as it opens.
func newControlledKube() (*kube, <-chan *watch.FakeWatcher) {
	client := fake.NewClientset()
	opened := make(chan *watch.FakeWatcher, 16)
	client.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, watch.Interface, error) {
		w := watch.NewFakeWithChanSize(1, false)
		opened <- w
		return true, w, nil
	})
	return &kube{opts: options{Namespace: "default"}, client: client}, opened
}

// Intent: a watch that closes right after opening is reported, so config
// retries it with backoff instead of Next reopening it in a tight loop.
func TestWatcherReportsWatchClosedRightAfterOpening(t *testing.T) {
	k, opened := newControlledKube()
	w, err := newWatcher(k)
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	defer w.Stop()

	(<-opened).Stop()
	if _, err := w.Next(); !errors.Is(err, errWatchClosed) {
		t.Fatalf("Next error = %v, want errWatchClosed", err)
	}

	// The next call opens a new watch and reads from it.
	go func() {
		(<-opened).Add(&v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "app"},
			Data:       map[string]string{"app.yaml": "port: 80"},
		})
	}()
	kvs, err := w.Next()
	if err != nil || len(kvs) != 1 {
		t.Fatalf("Next = %v, %v; want the ConfigMap from the new watch", kvs, err)
	}
}

// Intent: the API server ending a watch that has been open a while is routine;
// Next reopens it without reporting an error.
func TestWatcherReopensWatchEndedByServer(t *testing.T) {
	k, opened := newControlledKube()
	source, err := newWatcher(k)
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	w := source.(*watcher)
	defer w.Stop()
	first := <-opened
	w.mu.Lock()
	w.opened = time.Now().Add(-time.Minute)
	w.mu.Unlock()

	go func() {
		first.Stop()
		(<-opened).Add(&v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "app"},
			Data:       map[string]string{"app.yaml": "port: 80"},
		})
	}()
	kvs, err := w.Next()
	if err != nil || len(kvs) != 1 {
		t.Fatalf("Next = %v, %v; want the ConfigMap from the reopened watch", kvs, err)
	}
}

// Intent: an error event keeps the API status, such as 410 Gone, instead of
// reporting the Status object as a wrong type.
func TestWatcherReportsErrorEventStatus(t *testing.T) {
	k, opened := newControlledKube()
	w, err := newWatcher(k)
	if err != nil {
		t.Fatalf("newWatcher: %v", err)
	}
	defer w.Stop()

	(<-opened).Error(&apierrors.NewResourceExpired("too old resource version").ErrStatus)
	_, err = w.Next()
	if !apierrors.IsResourceExpired(err) {
		t.Fatalf("Next error = %v, want a resource expired status", err)
	}
}

// Intent: Load reuses the client the watcher holds, so a second Load, such as
// a config retry, does not replace it while the watcher reads it.
func TestLoadReusesClient(t *testing.T) {
	k := newTestKube()
	client := k.client

	if _, err := k.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if k.client != client {
		t.Fatal("Load replaced the client")
	}
}
