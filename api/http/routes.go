package http

import (
	"context"
	"net/http"
	"time"

	"github.com/tbright/log-router/internal/app"
)

type ingestRequest struct {
	Body struct {
		Timestamp time.Time `json:"timestamp"`
		Source    string    `json:"log_source"`
		Level     string    `json:"level"`
		Message   string    `json:"message"`
		Service   string    `json:"service"`
	}
}

type ingestResponse struct {
	Status int
	Body   struct {
		ErrorMessage string `json:"error,omitempty"`
	}
}

func HandleVectorIngest(application app.Application) RouteHandler[ingestRequest, ingestResponse] {
	return func(ctx context.Context, input *ingestRequest) (*ingestResponse, error) {
		entry := &app.VectorIngestLogEntry{
			Timestamp: input.Body.Timestamp,
			Source:    input.Body.Source,
			Level:     input.Body.Level,
			Message:   input.Body.Message,
			Service:   input.Body.Service,
		}
		err := application.HandleVectorIngest(ctx, entry)
		if err != nil {
			return &ingestResponse{
				Status: 500,
				Body: struct {
					ErrorMessage string `json:"error,omitempty"`
				}{
					ErrorMessage: err.Error(),
				},
			}, nil
		}

		return &ingestResponse{Status: http.StatusAccepted}, nil
	}
}

type askRequest struct {
	Question    string `json:"question"`
	NumLogLines int    `json:"num_log_lines"` // number of recent log lines to include
}

type askResponse struct {
	Status int
	Body   struct {
		Analysis     string `json:"analysis"`
		ErrorMessage string `json:"error,omitempty"`
	}
}

func HandleAsk(application app.Application) RouteHandler[askRequest, askResponse] {
	return func(ctx context.Context, input *askRequest) (*askResponse, error) {
		analysis, err := application.HandleAsk(ctx, input.Question, input.NumLogLines)
		if err != nil {
			return &askResponse{
				Status: 500,
				Body: struct {
					Analysis     string `json:"analysis"`
					ErrorMessage string `json:"error,omitempty"`
				}{
					ErrorMessage: err.Error(),
				},
			}, nil
		}
		return &askResponse{
			Status: 200,
			Body: struct {
				Analysis     string `json:"analysis"`
				ErrorMessage string `json:"error,omitempty"`
			}{
				Analysis: analysis,
			},
		}, nil
	}
}

type healthRequest struct{}
type healthResponse struct {
	Status int
	Body   struct {
		Status string `json:"status"`
	}
}

func HandleHealth() RouteHandler[healthRequest, healthResponse] {
	return func(ctx context.Context, input *healthRequest) (*healthResponse, error) {
		return &healthResponse{
			Body: struct {
				Status string `json:"status"`
			}{
				Status: "ok",
			},
		}, nil
	}
}
