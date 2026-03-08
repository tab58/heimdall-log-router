package http

import (
	"context"

	"github.com/danielgtaylor/huma/v2"
)

// RegisterRouteArgs is the arguments for the RegisterRoute function
type RegisterRouteArgs[I, O any] struct {
	API       huma.API
	Operation huma.Operation
	Handler   RouteHandler[I, O]
}

// RouteHandler is the type of handler to register for the route
type RouteHandler[I, O any] func(context.Context, *I) (*O, error)

// RegisterRoute registers a route with the given options.
func RegisterRoute[I, O any](args RegisterRouteArgs[I, O]) {
	api := args.API
	op := args.Operation
	handler := args.Handler

	huma.Register(api, op, func(ctx context.Context, input *I) (*O, error) {
		// call the handler
		return handler(ctx, input)
	})
}
