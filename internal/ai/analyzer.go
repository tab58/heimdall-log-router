package ai

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/tbright/log-router/internal/store"
)

const (
	AnthropicModel = "claude-sonnet-4-5-20250514"
	MaxTokens      = 1024
)

const systemPrompt = `You are a local development log analyzer. You receive error logs and surrounding context from multiple services running on a developer's machine.

Your job:
1. Identify the root cause of the error
2. Check if logs from other services show correlated failures
3. Provide a concise diagnosis and actionable fix

Format your response as:
**Root Cause**: One-line summary
**Correlated Issues**: Any related errors from other services (or "None")
**Fix**: Step-by-step resolution
**Code**: If applicable, show the fix`

type Analyzer struct {
	client anthropic.Client
}

func NewAnalyzer(apiKey string) Analyzer {
	client := anthropic.NewClient(
		option.WithAPIKey(apiKey),
	)
	return Analyzer{client: client}
}

func (a Analyzer) AnalyzeError(ctx context.Context, errorEntry store.LogEntry, contextEntries []store.LogEntry) (string, error) {
	prompt := buildPrompt(errorEntry, contextEntries)

	resp, err := a.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     AnthropicModel,
		MaxTokens: MaxTokens,
		System: []anthropic.TextBlockParam{
			{Text: systemPrompt},
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(
				anthropic.NewTextBlock(prompt),
			),
		},
	})
	if err != nil {
		return "", fmt.Errorf("claude api call failed: %w", err)
	}

	var result strings.Builder
	for _, block := range resp.Content {
		if block.Type == "text" {
			result.WriteString(block.Text)
		}
	}
	return result.String(), nil
}

func (a Analyzer) Ask(ctx context.Context, question string, contextEntries []store.LogEntry) (string, error) {
	prompt := buildAskPrompt(question, contextEntries)

	resp, err := a.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     AnthropicModel,
		MaxTokens: MaxTokens,
		System: []anthropic.TextBlockParam{
			{Text: systemPrompt},
		},
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(
				anthropic.NewTextBlock(prompt),
			),
		},
	})
	if err != nil {
		return "", fmt.Errorf("claude api call failed: %w", err)
	}

	var result strings.Builder
	for _, block := range resp.Content {
		if block.Type == "text" {
			result.WriteString(block.Text)
		}
	}
	return result.String(), nil
}

func buildPrompt(errorEntry store.LogEntry, contextEntries []store.LogEntry) string {
	var b strings.Builder

	b.WriteString("## Error\n")
	b.WriteString(fmt.Sprintf("**Source**: %s\n", errorEntry.Source))
	b.WriteString(fmt.Sprintf("**Time**: %s\n", errorEntry.Timestamp.Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("**Message**: %s\n\n", errorEntry.Message))

	b.WriteString("## Recent logs from all services (chronological)\n")
	b.WriteString("```\n")
	for _, entry := range contextEntries {
		b.WriteString(fmt.Sprintf("[%s] %s %s: %s\n",
			entry.Timestamp.Format("15:04:05.000"),
			entry.Source,
			strings.ToUpper(entry.Level),
			entry.Message,
		))
	}
	b.WriteString("```\n")

	return b.String()
}

func buildAskPrompt(question string, contextEntries []store.LogEntry) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("## Question\n%s\n\n", question))

	b.WriteString("## Recent logs from all services (chronological)\n")
	b.WriteString("```\n")
	for _, entry := range contextEntries {
		b.WriteString(fmt.Sprintf("[%s] %s %s: %s\n",
			entry.Timestamp.Format("15:04:05.000"),
			entry.Source,
			strings.ToUpper(entry.Level),
			entry.Message,
		))
	}
	b.WriteString("```\n")

	return b.String()
}
