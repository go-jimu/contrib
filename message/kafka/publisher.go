package kafka

import (
	"context"

	"github.com/go-jimu/components/ddd/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type producerClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

type publisher struct {
	client producerClient
	cfg    config
}

func NewPublisher(client *kgo.Client, opts ...Option) message.Publisher {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	var producer producerClient
	if client != nil {
		producer = client
	}
	return newPublisher(producer, cfg)
}

func newPublisher(client producerClient, cfg config) message.Publisher {
	return &publisher{
		client: client,
		cfg:    cfg,
	}
}

func (p *publisher) Publish(ctx context.Context, msg message.Message) error {
	if p.client == nil {
		return ErrNilClient
	}

	record, err := messageToRecord(msg, p.cfg)
	if err != nil {
		return err
	}

	attempts := p.cfg.publishRetry.maxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	var publishErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		publishErr = p.client.ProduceSync(ctx, record).FirstErr()
		if publishErr == nil {
			return nil
		}
		if attempt == attempts {
			break
		}
		if p.cfg.publishRetry.backoff != nil && p.cfg.publishRetry.sleep != nil {
			if err := p.cfg.publishRetry.sleep(ctx, p.cfg.publishRetry.backoff(attempt)); err != nil {
				return err
			}
		}
	}
	return publishErr
}

func NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(opts...)
}
