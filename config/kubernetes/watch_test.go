package kubernetes

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func newTestKube() *kube {
	return &kube{opts: options{Namespace: "default"}, client: fake.NewSimpleClientset()}
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
