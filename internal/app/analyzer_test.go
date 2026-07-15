package app

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tab58/tenzing-agent-harness/pkg/tenzing"

	"github.com/tab58/heimdall-log-router/internal/app/agent"
	"github.com/tab58/heimdall-log-router/internal/stream"
)

// mockLLM implements tenzing.LLM. The analyzer always registers a text delta
// callback, so the harness takes the streaming path: SendStreamingMessage
// emits streamEvents then closes the channel.
type mockLLM struct {
	streamEvents []tenzing.StreamEvent
}

func (m *mockLLM) ProviderName() tenzing.Provider { return tenzing.ProviderAnthropic }

func (m *mockLLM) SendSyncMessage(_ context.Context, _ tenzing.CompletionRequest) (tenzing.CompletionResponse, error) {
	return tenzing.CompletionResponse{}, nil
}

func (m *mockLLM) SendStreamingMessage(ctx context.Context, _ tenzing.CompletionRequest, events chan<- tenzing.StreamEvent) error {
	defer close(events)
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, e := range m.streamEvents {
		events <- e
	}
	return nil
}

func (m *mockLLM) SendMessageWithTools(_ context.Context, _ tenzing.CompletionRequest, _ []tenzing.LLMToolDefinition) (tenzing.CompletionResponse, error) {
	return tenzing.CompletionResponse{}, nil
}

func (m *mockLLM) CountTokens(_ context.Context, _ tenzing.CompletionRequest) (tenzing.TokenCount, error) {
	return tenzing.TokenCount{InputTokens: 10}, nil
}

func (m *mockLLM) ListModels(_ context.Context) ([]tenzing.ModelInfo, error) {
	return nil, nil
}

func (m *mockLLM) GetCurrentModel() string   { return "test-model" }
func (m *mockLLM) GetContextWindowSize() int { return 128000 }

// answeringLLM returns a mock that streams chunks and finishes with a final
// response whose text is the concatenation of chunks.
func answeringLLM(chunks ...string) *mockLLM {
	full := strings.Join(chunks, "")
	events := []tenzing.StreamEvent{{Type: tenzing.StreamEventStart}}
	for _, c := range chunks {
		events = append(events, tenzing.StreamEvent{Type: tenzing.StreamEventDelta, Text: c})
	}
	events = append(events, tenzing.StreamEvent{
		Type: tenzing.StreamEventStop,
		Response: &tenzing.CompletionResponse{
			ID:         "resp-1",
			Model:      "test-model",
			StopReason: tenzing.StopReasonEndTurn,
			Content:    []tenzing.ContentBlock{tenzing.NewTextContent(full)},
			Usage:      tenzing.Usage{InputTokens: 100, OutputTokens: 20},
		},
	})
	return &mockLLM{streamEvents: events}
}

// testModel is the model definition handed to the analyzer in tests; the
// factory below ignores it and returns the mock.
var testModel = tenzing.ModelDefinition{
	Provider:          tenzing.ProviderAnthropic,
	Name:              "test-model",
	MaxTokens:         4096,
	ContextWindowSize: 128000,
}

// newTestAnalyzer builds an Analyzer whose harness always reasons with m.
func newTestAnalyzer(m *mockLLM, progress agent.ProgressFunc, delta agent.DeltaFunc) agent.Analyzer {
	factory := func(_ tenzing.ModelDefinition) (tenzing.LLM, error) { return m, nil }
	return agent.NewAnalyzer(testModel, factory, nil, progress, delta, nil)
}

func TestAnalyzerProcess(t *testing.T) {
	want := "Root Cause: disk full\nFix: clear /tmp"

	a := newTestAnalyzer(answeringLLM(want), nil, nil)
	e := stream.Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "db",
		Message:   "no space left on device",
	}
	got, err := a.Process(context.Background(), "test-id", []stream.Event{e})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAnalyzerProcessWithContext(t *testing.T) {
	want := "Root Cause: cascading timeout"

	a := newTestAnalyzer(answeringLLM(want), nil, nil)
	trigger := stream.Event{
		Timestamp: time.Now(),
		Severity:  "error",
		Service:   "api",
		Message:   "request timeout",
	}
	ctxEvents := []stream.Event{
		{Timestamp: time.Now().Add(-2 * time.Second), Severity: "warn", Service: "db", Message: "slow query"},
		{Timestamp: time.Now().Add(-1 * time.Second), Severity: "warn", Service: "cache", Message: "cache miss"},
	}

	got, err := a.Process(context.Background(), "test-id", append(ctxEvents, trigger))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestAnalyzerProcessCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	a := newTestAnalyzer(answeringLLM("never returned"), nil, nil)
	_, err := a.Process(ctx, "test-id", []stream.Event{{Severity: "error"}})
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

func TestAnalyzerStreamsAssistantDeltas(t *testing.T) {
	chunks := []string{"hello ", "world"}

	var mu sync.Mutex
	var got []string
	delta := func(s string) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, s)
	}

	a := newTestAnalyzer(answeringLLM(chunks...), nil, delta)
	result, err := a.Process(context.Background(), "tid", []stream.Event{{Severity: "error", Message: "boom"}})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if result != "hello world" {
		t.Errorf("result = %q, want %q", result, "hello world")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(chunks) {
		t.Fatalf("delta calls = %d, want %d", len(got), len(chunks))
	}
	for i, c := range chunks {
		if got[i] != c {
			t.Errorf("delta[%d] = %q, want %q", i, got[i], c)
		}
	}
}

func TestAnalyzerEmitsProgress(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	progress := func(msg string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, msg)
	}

	a := newTestAnalyzer(answeringLLM("done"), progress, nil)
	if _, err := a.Process(context.Background(), "test-id", []stream.Event{{Severity: "error", Service: "db"}}); err != nil {
		t.Fatalf("Process: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"analysis started", "analysis completed"} {
		if !strings.Contains(joined, want) {
			t.Errorf("progress missing %q\nlines:\n%s", want, joined)
		}
	}
}

func TestRenderSystemPromptListsCodeSearchDirs(t *testing.T) {
	dirs := []string{"services/megatron-api", "environment/shared/golang"}

	got := agent.RenderSystemPrompt(dirs)

	if !strings.Contains(got, "The source code lives in these folders") {
		t.Errorf("prompt missing folder-list header\nprompt:\n%s", got)
	}
	for _, d := range dirs {
		if !strings.Contains(got, "- "+d) {
			t.Errorf("prompt missing dir %q", d)
		}
	}
}

func TestRenderSystemPromptOmitsFolderListWhenEmpty(t *testing.T) {
	for _, dirs := range [][]string{nil, {}} {
		got := agent.RenderSystemPrompt(dirs)
		if strings.Contains(got, "lives in these folders") {
			t.Errorf("prompt should omit folder list for dirs=%v\nprompt:\n%s", dirs, got)
		}
	}
}

func TestBuildPromptContainsErrorDetails(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	e := stream.Event{
		Timestamp: ts,
		Severity:  "error",
		Service:   "payments",
		Message:   "charge failed: card declined",
	}

	got := agent.BuildPrompt(e, nil)

	checks := []string{
		"payments",
		"charge failed: card declined",
		ts.Format(time.RFC3339),
	}
	for _, want := range checks {
		if !strings.Contains(got, want) {
			t.Errorf("prompt missing %q\nprompt:\n%s", want, got)
		}
	}
}

func TestBuildPromptIncludesContextLogs(t *testing.T) {
	e := stream.Event{Timestamp: time.Now(), Severity: "error", Service: "api", Message: "boom"}
	ctx := []stream.Event{
		{Timestamp: time.Now(), Severity: "warn", Service: "db", Message: "slow query"},
		{Timestamp: time.Now(), Severity: "info", Service: "cache", Message: "eviction"},
	}

	got := agent.BuildPrompt(e, ctx)

	for _, want := range []string{"slow query", "eviction", "db", "cache"} {
		if !strings.Contains(got, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
}
