package pubsubio

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"testing"
)

func TestRead_NilOptionsPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when opts is nil")
		}
	}()

	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	Read(s, "test-project", nil)
}

func TestRead_BothTopicAndSubscriptionPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when both topic and subscription are set")
		}
	}()

	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	opts := &ReadOptions{
		Topic:        "topic",
		Subscription: "sub",
	}
	Read(s, "test-project", opts)
}

func TestRead_NeitherTopicNorSubscriptionPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when neither topic nor subscription is set")
		}
	}()

	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	opts := &ReadOptions{}
	Read(s, "test-project", opts)
}
