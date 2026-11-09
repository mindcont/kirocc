package server

import (
	"net/http"
	"time"

	messagesapp "github.com/d-kuro/kirocc/internal/app/messages"
	"github.com/d-kuro/kirocc/internal/archive"
	"github.com/d-kuro/kirocc/internal/config"
	"github.com/d-kuro/kirocc/internal/kiroclient"
	"github.com/d-kuro/kirocc/internal/tracing"
	"github.com/d-kuro/kirocc/internal/websearch"
)

// ServerOption configures a Server.
type ServerOption func(*Server)

// WithAPIKeyMap associates gateway API keys with stable device IDs.
func WithAPIKeyMap(keys map[string]string) ServerOption {
	return func(s *Server) { s.apiKeyMap = cloneAPIKeyMap(keys) }
}

// WithArchive enables per-device, per-session JSONL conversation archives.
func WithArchive(store *archive.Store) ServerOption {
	return func(s *Server) { s.archive = store }
}

// WithOTel enables OpenTelemetry tracing middleware.
func WithOTel(bodyLimit int) ServerOption {
	return func(s *Server) {
		s.otel = true
		s.otelBodyLimit = bodyLimit
	}
}

// WithCapture enables upstream capture logging in the messages service.
func WithCapture(enabled bool) ServerOption {
	return func(s *Server) { s.captureEnabled = enabled }
}

// WithUpstreamDiag enables metadata-only upstream request diagnostics for the
// listed Claude Code session IDs.
func WithUpstreamDiag(sessionIDs []string) ServerOption {
	return func(s *Server) { s.upstreamDiagSessions = sessionIDs }
}

// WithWebSearch enables in-proxy emulation of the web_search_20250305 server
// tool through the given provider.
func WithWebSearch(provider websearch.Provider, maxResults int) ServerOption {
	return func(s *Server) {
		s.webSearch = provider
		s.webSearchMaxResults = maxResults
	}
}

// WithKeepAliveInterval sets the idle interval for streaming SSE comments.
// A zero duration disables keep-alive comments.
func WithKeepAliveInterval(interval time.Duration) ServerOption {
	return func(s *Server) { s.keepAliveInterval = interval }
}

// WithMaxRequestBody caps the client request body in bytes. Zero disables the cap.
func WithMaxRequestBody(limit int64) ServerOption {
	return func(s *Server) { s.maxRequestBody = limit }
}

// Server is the HTTP server for the kirocc proxy.
type Server struct {
	apiKey               string
	apiKeyMap            map[string]string // device ID -> key
	archive              *archive.Store
	otel                 bool
	otelBodyLimit        int
	captureEnabled       bool
	upstreamDiagSessions []string
	keepAliveInterval    time.Duration
	maxRequestBody       int64
	webSearch            websearch.Provider
	webSearchMaxResults  int
	mux                  *http.ServeMux
	messages             *messagesapp.Service
}

func cloneAPIKeyMap(keys map[string]string) map[string]string {
	if len(keys) == 0 {
		return nil
	}
	copy := make(map[string]string, len(keys))
	for deviceID, key := range keys {
		copy[deviceID] = key
	}
	return copy
}

// New creates a new Server.
func New(authMgr messagesapp.TokenGetter, apiKey string, client kiroclient.Client, opts ...ServerOption) *Server {
	s := &Server{
		apiKey:         apiKey,
		mux:            http.NewServeMux(),
		maxRequestBody: config.DefaultMaxRequestBody,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.messages = messagesapp.New(authMgr, client,
		messagesapp.WithArchive(s.archive),
		messagesapp.WithCapture(s.captureEnabled),
		messagesapp.WithUpstreamDiag(s.upstreamDiagSessions),
		messagesapp.WithKeepAliveInterval(s.keepAliveInterval),
		messagesapp.WithMaxRequestBody(s.maxRequestBody),
		messagesapp.WithWebSearch(s.webSearch, s.webSearchMaxResults),
	)
	s.registerRoutes()
	return s
}

// Handler returns the http.Handler for the server.
func (s *Server) Handler() http.Handler {
	h := traceMiddleware(corsMiddleware(s.authMiddleware(s.mux)))
	if s.otel {
		h = tracing.Middleware(h, s.otelBodyLimit)
	}
	return h
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /v1/models", s.handleModels)
	s.mux.HandleFunc("POST /v1/messages/count_tokens", s.messages.HandleCountTokens)
	s.mux.HandleFunc("POST /v1/messages", s.messages.HandleMessages)
}
