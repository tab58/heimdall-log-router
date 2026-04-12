package agent

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/tab58/heimdall-log-router/internal/stream"
)

//go:embed prompts/system_prompt.go.tmpl
var systemPrompt string

// Analyzer implements agent.Agent using the Claude Code CLI (claude --print).
type Analyzer struct{}

// NewAnalyzer creates an Analyzer that shells out to the claude CLI.
func NewAnalyzer() Analyzer {
	return Analyzer{}
}

// Process sends the event window to the Claude Code CLI for analysis.
// It identifies the triggering event as the last error or fatal in events.
// It satisfies agent.Agent.
func (a Analyzer) Process(ctx context.Context, events []stream.Event) (string, error) {
	trigger := findTrigger(events)
	prompt := BuildPrompt(trigger, events)

	cmd := exec.CommandContext(ctx, "claude", "--print", "--system-prompt", systemPrompt)
	cmd.Stdin = strings.NewReader(prompt)

	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("claude exited %d: %s", exitErr.ExitCode(), strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", fmt.Errorf("claude: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
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
