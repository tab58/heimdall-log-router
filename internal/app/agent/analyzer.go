package agent

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"log"
	"strings"
	"text/template"
	"time"

	"github.com/tab58/tenzing-agent-harness/pkg/tenzing"

	"github.com/tab58/heimdall-log-router/internal/stream"
)

//go:embed prompts/system_prompt.go.tmpl
var systemPromptRaw string

var systemPromptTmpl = template.Must(template.New("system_prompt").Parse(systemPromptRaw))

// RenderSystemPrompt renders the analyzer system prompt. codeSearchDirs, when
// non-empty, is listed in the prompt as the folders to search for source code.
func RenderSystemPrompt(codeSearchDirs []string) string {
	var b strings.Builder
	if err := systemPromptTmpl.Execute(&b, struct{ CodeSearchDirs []string }{codeSearchDirs}); err != nil {
		// Unreachable with a static template and in-memory writer; keep the
		// raw prompt as a safe fallback rather than failing analysis.
		return systemPromptRaw
	}
	return b.String()
}

// heartbeatInterval is how often the analyzer emits "still running" progress
// updates while an analysis turn is in flight.
const heartbeatInterval = 5 * time.Second

// disabledTools removes write/exec capability from the harness. Heimdall
// feeds untrusted log content into the prompt; the analyzer is read-only.
var disabledTools = []string{"bash", "edit"}

// ProgressFunc is invoked with human-readable status strings (tool use,
// heartbeat, token counts) while analysis is running. It is called
// synchronously from a background goroutine; it should not block.
type ProgressFunc func(msg string)

// DeltaFunc is invoked with each assistant text chunk as it streams out of
// the harness. It lets callers render a live token feed. Called
// synchronously; should not block.
type DeltaFunc func(delta string)

// Analyzer implements agent.Agent using the tenzing harness in-process
// agentic loop.
type Analyzer struct {
	model        tenzing.ModelDefinition
	llmFactory   func(tenzing.ModelDefinition) (tenzing.LLM, error)
	systemPrompt string
	logger       *log.Logger
	progress     ProgressFunc
	delta        DeltaFunc
}

// NewAnalyzer creates an Analyzer that runs the tenzing harness loop against
// model. llmFactory (may be nil) overrides how the harness builds LLM
// clients; nil uses the harness default, which resolves API keys from
// provider env vars. logger receives progress lines on stderr; progress (may
// be nil) receives the same lines so they can be broadcast to clients; delta
// (may be nil) receives each assistant text chunk so the caller can stream
// the response live. codeSearchDirs (may be nil) is rendered into the system
// prompt as the list of folders to search for source code. The harness's
// Read/Glob/Grep tools resolve against the process working directory (main
// chdirs to HEIMDALL_WORKSPACE_DIR at startup when set).
func NewAnalyzer(model tenzing.ModelDefinition, llmFactory func(tenzing.ModelDefinition) (tenzing.LLM, error), logger *log.Logger, progress ProgressFunc, delta DeltaFunc, codeSearchDirs []string) Analyzer {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	return Analyzer{
		model:        model,
		llmFactory:   llmFactory,
		systemPrompt: RenderSystemPrompt(codeSearchDirs),
		logger:       logger,
		progress:     progress,
		delta:        delta,
	}
}

// Process sends the event window through the agent_harness loop for analysis.
// It identifies the triggering event as the last error or fatal in events.
// It satisfies agent.Agent.
func (a Analyzer) Process(ctx context.Context, id string, events []stream.Event) (string, error) {
	trigger := findTrigger(events)
	prompt := BuildPrompt(trigger, events)
	short := shortID(id)
	start := time.Now()

	a.emit(fmt.Sprintf("[%s] analysis started: service=%q severity=%s events=%d",
		short, trigger.Service, trigger.Severity, len(events)))

	opts := []tenzing.Option{
		tenzing.WithSystemPrompt(a.systemPrompt),
		tenzing.WithTextDeltaHandler(func(text string) {
			if a.delta != nil {
				a.delta(text)
			}
		}),
		tenzing.WithHooks(tenzing.Hooks{
			OnToolExecutionStarted: func(ev tenzing.ToolExecutionStartedEvent) {
				a.emit(fmt.Sprintf("[%s] tool: %s %s", short, ev.ToolName, truncate(ev.Input, 160)))
			},
			OnReasoningStarted: func(ev tenzing.ReasoningStartedEvent) {
				a.emit(fmt.Sprintf("[%s] reasoning iteration %d", short, ev.Iteration))
			},
			OnReasoningFinished: func(ev tenzing.ReasoningFinishedEvent) {
				a.emit(fmt.Sprintf("[%s] reasoning done (model=%s in=%d out=%d)",
					short, ev.Model, ev.InputTokens, ev.OutputTokens))
			},
			OnToolFailed: func(ev tenzing.ToolFailedEvent) {
				a.emit(fmt.Sprintf("[%s] tool %s failed: %s", short, ev.ToolName, ev.Error))
			},
			OnError: func(ev tenzing.ErrorEvent) {
				a.emit(fmt.Sprintf("[%s] error: %s", short, ev.Error))
			},
		}),
	}
	for _, tool := range disabledTools {
		opts = append(opts, tenzing.WithDisabledTool(tool))
	}
	if a.llmFactory != nil {
		opts = append(opts, tenzing.WithLLMFactory(a.llmFactory))
	}

	h, err := tenzing.New(a.model, opts...)
	if err != nil {
		return "", fmt.Errorf("create harness: %w", err)
	}
	defer h.Shutdown()

	// Heartbeat while the turn is in flight so the monitor keeps a pulse
	// during long tool calls or slow LLM responses.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				a.emit(fmt.Sprintf("[%s] analysis running, %s elapsed", short, elapsed(start)))
			}
		}
	}()

	result, err := h.RunTurn(ctx, prompt)
	close(done)
	if err != nil {
		a.emit(fmt.Sprintf("[%s] analysis failed after %s: %v", short, elapsed(start), err))
		return "", fmt.Errorf("analysis: %w", err)
	}

	a.emit(fmt.Sprintf("[%s] analysis completed in %s", short, elapsed(start)))
	return strings.TrimSpace(result), nil
}

// truncate shortens s to at most n runes, appending an ellipsis if cut. Used
// for keeping streaming progress lines compact.
func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// shortID returns the first 8 characters of id, matching the monitor's
// display shortening. Empty or short ids pass through unchanged.
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// emit writes msg to the analyzer's logger (stderr) and the progress callback
// (client broadcast) when present.
func (a Analyzer) emit(msg string) {
	a.logger.Print(msg)
	if a.progress != nil {
		a.progress(msg)
	}
}

func elapsed(start time.Time) time.Duration {
	return time.Since(start).Round(time.Second)
}

// findTrigger returns the last error or fatal event in events.
// Falls back to the last event, or a zero Event if the slice is empty.
func findTrigger(events []stream.Event) stream.Event {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Severity == "error" || events[i].Severity == "fatal" {
			return events[i]
		}
	}
	if len(events) > 0 {
		return events[len(events)-1]
	}
	return stream.Event{}
}

func BuildPrompt(e stream.Event, ctxEvents []stream.Event) string {
	var b strings.Builder

	b.WriteString("## Error\n")
	fmt.Fprintf(&b, "**Service**: %s\n", e.Service)
	fmt.Fprintf(&b, "**Time**: %s\n", e.Timestamp.Format(time.RFC3339))
	fmt.Fprintf(&b, "**Message**: %s\n\n", e.Message)

	b.WriteString("## Recent logs from all services (chronological)\n")
	b.WriteString("```\n")
	for _, entry := range ctxEvents {
		fmt.Fprintf(&b, "[%s] %s %s: %s\n",
			entry.Timestamp.Format("15:04:05.000"),
			entry.Service,
			strings.ToUpper(entry.Severity),
			entry.Message)
	}
	b.WriteString("```\n")

	return b.String()
}
