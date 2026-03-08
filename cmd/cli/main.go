package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const defaultAgentURL = "http://localhost:7077"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "ask":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: logrouter ask \"your question\"")
			os.Exit(1)
		}
		question := strings.Join(os.Args[2:], " ")
		if err := askAgent(question); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "health":
		if err := checkHealth(); err != nil {
			fmt.Fprintf(os.Stderr, "Agent is not running: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Agent is healthy")
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func askAgent(question string) error {
	agentURL := getAgentURL()

	body, err := json.Marshal(map[string]interface{}{
		"question": question,
		"context":  100,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := http.Post(agentURL+"/ask", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to reach agent at %s (is it running?): %w", agentURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("agent returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Analysis string `json:"analysis"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	fmt.Println(result.Analysis)
	return nil
}

func checkHealth() error {
	agentURL := getAgentURL()

	resp, err := http.Get(agentURL + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: status %d", resp.StatusCode)
	}
	return nil
}

func getAgentURL() string {
	if url := os.Getenv("LOGROUTER_AGENT_URL"); url != "" {
		return url
	}
	return defaultAgentURL
}

func printUsage() {
	fmt.Println(`Usage: logrouter <command> [args]

Commands:
  ask <question>   Ask AI about recent logs
                   Example: logrouter ask "why are requests timing out?"

  health           Check if the AI agent is running

  help             Show this help message

Environment:
  LOGROUTER_AGENT_URL   Agent URL (default: http://localhost:7077)
  ANTHROPIC_API_KEY     Required for the agent server`)
}
