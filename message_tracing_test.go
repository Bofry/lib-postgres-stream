package postgres

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/Bofry/trace"
	"github.com/jackc/pglogrepl"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMessageTraceBodyLengthLimit(t *testing.T) {
	const limit = 5
	cases := []struct {
		name string
		body string
		want string
	}{
		{"replacement_runes", strings.Repeat("\uFFFD", 1000), strings.Repeat("\uFFFD", limit)},
		{"mixed_replacement_runes", "ab" + strings.Repeat("\uFFFD", 100) + "cdef", "ab\uFFFD\uFFFD\uFFFD"},
		{"replacement_after_limit", "abcde" + strings.Repeat("\uFFFD", 100), "abcde"},
		{"ascii", "abcdefgh", "abcde"},
		{"unicode", "你好世界朋友", "你好世界朋"},
		{"invalid_utf8", "a\xffb\xfecdef", "abcde"},
		{"short_body", "a\uFFFD界", "a\uFFFD界"},
		{"empty_body", "", ""},
	}

	for _, tc := range cases {
		for _, path := range []string{"argv", "reply"} {
			t.Run(tc.name+"/"+path, func(t *testing.T) {
				recorder := tracetest.NewSpanRecorder()
				limits := sdktrace.NewSpanLimits()
				limits.AttributeValueLengthLimit = limit
				limits.AttributeCountLimit = 128
				provider := sdktrace.NewTracerProvider(
					sdktrace.WithSampler(sdktrace.AlwaysSample()),
					sdktrace.WithSpanLimits(limits),
					sdktrace.WithSpanProcessor(recorder),
				)
				t.Cleanup(func() {
					if err := provider.Shutdown(context.Background()); err != nil {
						t.Error(err)
					}
				})
				message := &Message{data: &pglogrepl.XLogData{WALData: []byte(tc.body)}}
				span := trace.CreateSeverityTracer(provider.Tracer("message-test")).Start(context.Background(), "message")
				if path == "argv" {
					span.Argv(message)
				} else {
					span.Reply("ok", message)
				}
				span.End()

				spans := recorder.Ended()
				if len(spans) != 1 {
					t.Fatalf("recorded %d spans, want 1", len(spans))
				}
				if string(message.Body()) != tc.body {
					t.Fatal("tracing changed the original message body")
				}
				for _, attr := range spans[0].Attributes() {
					if string(attr.Key) != path+".body" {
						continue
					}
					got := attr.Value.AsString()
					if got != tc.want {
						t.Fatalf("stored %d runes / %d bytes, want %q (%d runes)", utf8.RuneCountInString(got), len(got), tc.want, utf8.RuneCountInString(tc.want))
					}
					return
				}
				t.Fatalf("missing %s.body attribute", path)
			})
		}
	}
}
