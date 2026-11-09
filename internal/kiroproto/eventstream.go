package kiroproto

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"time"

	"github.com/d-kuro/kirocc/internal/logging"
)

// Event represents a parsed event from the Kiro event stream.
type Event struct {
	Type         string // event type from header
	Content      string // assistantResponseEvent → content field
	ThinkingText string // reasoningContentEvent → text field
	ToolName     string // toolUseEvent
	ToolUseID    string
	ToolInput    string
	ToolStop     bool
	Credits      float64 // meteringEvent → usage field
	InputTokens  int     // meteringEvent or metadataEvent
	OutputTokens int     // meteringEvent or metadataEvent
	// metadataEvent token breakdown
	UncachedInputTokens   int
	CacheReadInputTokens  int
	CacheWriteInputTokens int
	TotalTokens           int
	// reasoningContentEvent
	Signature       string
	RedactedContent string // base64-encoded
	// contextUsageEvent
	ContextUsagePercentage float64
	// messageMetadataEvent
	ConversationID string
	UtteranceID    string
	// invalidStateEvent
	InvalidStateReason string
	ErrorMessage       string
}

// ErrorText returns the error message, falling back to InvalidStateReason.
func (e Event) ErrorText() string {
	if e.ErrorMessage != "" {
		return e.ErrorMessage
	}
	return e.InvalidStateReason
}

// Kiro API event type constants.
const (
	EventAssistantResponse  = "assistantResponseEvent"
	EventReasoningContent   = "reasoningContentEvent"
	EventToolUse            = "toolUseEvent"
	EventMetadata           = "metadataEvent"
	EventMetering           = "meteringEvent"
	EventInvalidState       = "invalidStateEvent"
	EventException          = "exception"
	EventMessageMetadata    = "messageMetadataEvent"
	EventFollowupPrompt     = "followupPromptEvent"
	EventCitation           = "citationEvent"
	EventCode               = "codeEvent"
	EventCodeReference      = "codeReferenceEvent"
	EventSupplementaryLinks = "supplementaryWebLinksEvent"
	EventIntents            = "intentsEvent"
	EventInteractionComps   = "interactionComponentsEvent"
	EventDryRunSucceed      = "dryRunSucceedEvent"
	EventContextUsage       = "contextUsageEvent"
	EventInitialResponse    = "initial-response"
)

// ParseStream reads AWS Event Stream binary frames from r and calls callback for each parsed Event.
// If callback returns true, parsing stops and ParseStream returns nil (early stop).
func ParseStream(ctx context.Context, r io.Reader, callback func(Event) bool) (result error) {
	started := time.Now()
	_, short := logging.TraceIDs(ctx)
	var total, assistant, reasoning, tools, other, contentBytes, thinkingBytes, toolBytes int
	var frames, frameAssistant, frameReasoning, frameTool, frameMetadata, frameMetering, frameInitial, frameIgnored, frameUnknown int
	var frameMessageMetadata, frameContextUsage int
	var stopped bool
	originalCallback := callback
	callback = func(e Event) bool {
		total++
		contentBytes += len(e.Content)
		thinkingBytes += len(e.ThinkingText) + len(e.RedactedContent)
		toolBytes += len(e.ToolInput)
		switch e.Type {
		case EventAssistantResponse:
			assistant++
		case EventReasoningContent:
			reasoning++
		case EventToolUse:
			tools++
		default:
			other++
		}
		stopped = originalCallback(e)
		return stopped
	}
	defer func() {
		reason := "eof"
		switch {
		case ctx.Err() == context.DeadlineExceeded:
			reason = "context_deadline"
		case ctx.Err() != nil:
			reason = "context_canceled"
		case result != nil:
			reason = "parse_error"
		case stopped:
			reason = "callback_stop"
		}
		slog.InfoContext(ctx, "upstream stream summary", "trace_id", short, "session_id", logging.ShortID(logging.SessionIDFromContext(ctx)), "reason", reason, "elapsed_ms", time.Since(started).Milliseconds(), "events", total, "assistant_events", assistant, "reasoning_events", reasoning, "tool_events", tools, "other_events", other, "content_bytes", contentBytes, "thinking_bytes", thinkingBytes, "tool_bytes", toolBytes, "frames", frames, "frame_assistant", frameAssistant, "frame_reasoning", frameReasoning, "frame_tool", frameTool, "frame_metadata", frameMetadata, "frame_metering", frameMetering, "frame_initial", frameInitial, "frame_ignored", frameIgnored, "frame_unknown", frameUnknown, "frame_message_metadata", frameMessageMetadata, "frame_context_usage", frameContextUsage)
	}()
	br := bufio.NewReader(r)
	var acc toolUseAccumulator

	for {
		// Check for context cancellation before reading the next frame.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		headers, payload, err := readFrame(br)
		if err == io.EOF {
			// Flush any in-progress tool call that never received a stop frame.
			if event, ok := acc.flush(); ok {
				callback(event)
			}
			return nil
		}
		if err != nil {
			return err
		}

		msgType, eventType := extractFrameHeaders(headers)
		frames++
		switch eventType {
		case EventAssistantResponse:
			frameAssistant++
		case EventReasoningContent:
			frameReasoning++
		case EventToolUse:
			frameTool++
		case EventMetadata:
			frameMetadata++
		case EventMetering:
			frameMetering++
		case EventInitialResponse:
			frameInitial++
		case EventMessageMetadata:
			frameMessageMetadata++
		case EventContextUsage:
			frameContextUsage++
		case EventInvalidState, EventFollowupPrompt, EventCitation, EventCode, EventCodeReference, EventSupplementaryLinks, EventIntents, EventInteractionComps, EventDryRunSucceed:
			frameIgnored++
		default:
			frameUnknown++
		}

		// Handle exception frames (stream-level errors).
		if msgType == "exception" {
			var m struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal(payload, &m); err != nil {
				slog.Warn("kiro: failed to decode exception payload", "err", err)
			}
			callback(Event{
				Type:               EventException,
				ErrorMessage:       m.Message,
				InvalidStateReason: eventType,
			})
			return nil
		}

		var stop bool

		switch eventType {
		case EventAssistantResponse:
			var m struct {
				Content string `json:"content"`
			}
			if err := json.Unmarshal(payload, &m); err != nil {
				return fmt.Errorf("decode %s: %w", eventType, err)
			}
			stop = callback(Event{Type: eventType, Content: m.Content})

		case EventReasoningContent:
			var m struct {
				Text            string `json:"text"`
				Signature       string `json:"signature"`
				RedactedContent string `json:"redactedContent"`
			}
			if err := json.Unmarshal(payload, &m); err != nil {
				return fmt.Errorf("decode %s: %w", eventType, err)
			}
			stop = callback(Event{
				Type:            eventType,
				ThinkingText:    m.Text,
				Signature:       m.Signature,
				RedactedContent: m.RedactedContent,
			})

		case EventToolUse:
			var raw map[string]jsontext.Value
			if err := json.Unmarshal(payload, &raw); err != nil {
				slog.Warn("kiro: failed to decode toolUseEvent payload", "err", err)
				continue
			}
			if slices.ContainsFunc(acc.update(raw), callback) {
				stop = true
			}

		case EventMetadata:
			var m struct {
				TokenUsage struct {
					UncachedInputTokens   int `json:"uncachedInputTokens"`
					OutputTokens          int `json:"outputTokens"`
					TotalTokens           int `json:"totalTokens"`
					CacheReadInputTokens  int `json:"cacheReadInputTokens"`
					CacheWriteInputTokens int `json:"cacheWriteInputTokens"`
				} `json:"tokenUsage"`
			}
			if err := json.Unmarshal(payload, &m); err == nil {
				tu := m.TokenUsage
				stop = callback(Event{
					Type:                  eventType,
					UncachedInputTokens:   tu.UncachedInputTokens,
					CacheReadInputTokens:  tu.CacheReadInputTokens,
					CacheWriteInputTokens: tu.CacheWriteInputTokens,
					OutputTokens:          tu.OutputTokens,
					TotalTokens:           tu.TotalTokens,
					InputTokens:           tu.UncachedInputTokens + tu.CacheReadInputTokens,
				})
			} else {
				slog.Warn("kiro: failed to decode event", "type", eventType, "err", err)
			}

		case EventMetering:
			var m struct {
				Usage        float64 `json:"usage"`
				InputTokens  int     `json:"inputTokens"`
				OutputTokens int     `json:"outputTokens"`
			}
			if err := json.Unmarshal(payload, &m); err == nil {
				stop = callback(Event{
					Type:         eventType,
					Credits:      m.Usage,
					InputTokens:  m.InputTokens,
					OutputTokens: m.OutputTokens,
				})
			} else {
				slog.Warn("kiro: failed to decode event", "type", eventType, "err", err)
			}

		case EventInvalidState:
			var m struct {
				Reason  string `json:"reason"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal(payload, &m); err != nil {
				return fmt.Errorf("decode %s: %w", eventType, err)
			}
			stop = callback(Event{
				Type:               eventType,
				InvalidStateReason: m.Reason,
				ErrorMessage:       m.Message,
			})

		case EventMessageMetadata:
			var m struct {
				ConversationID string `json:"conversationId"`
				UtteranceID    string `json:"utteranceId"`
			}
			if err := json.Unmarshal(payload, &m); err == nil {
				stop = callback(Event{
					Type:           eventType,
					ConversationID: m.ConversationID,
					UtteranceID:    m.UtteranceID,
				})
			} else {
				slog.Warn("kiro: failed to decode event", "type", eventType, "err", err)
			}

		case EventContextUsage:
			var m struct {
				ContextUsagePercentage float64 `json:"contextUsagePercentage"`
			}
			if err := json.Unmarshal(payload, &m); err == nil {
				stop = callback(Event{
					Type:                   eventType,
					ContextUsagePercentage: m.ContextUsagePercentage,
				})
			} else {
				slog.Warn("kiro: failed to decode event", "type", eventType, "err", err)
			}

		case EventFollowupPrompt, EventCitation, EventCode, EventCodeReference,
			EventSupplementaryLinks, EventIntents, EventInteractionComps,
			EventDryRunSucceed, EventInitialResponse:
			// Known no-op events — silently ignore.

		default:
			if eventType != "" {
				p := string(payload[:min(len(payload), 200)])
				if len(payload) > 200 {
					p += "..."
				}
				slog.Warn("kiro: unknown event type (ignored)", "type", eventType, "payload", p)
			}
		}

		if stop {
			return nil
		}
	}
}
