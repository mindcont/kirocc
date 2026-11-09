package messages

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"

	"github.com/d-kuro/kirocc/internal/kiroproto"
	"github.com/d-kuro/kirocc/internal/logging"
)

// upstreamDiag emits metadata-only diagnostics for requests whose Claude Code
// session ID is in an operator-supplied allowlist. It never records message
// text, tool output, tokens or credentials: only sizes, counts, IDs and a hash
// of the exact bytes that were sent upstream.
type upstreamDiag struct {
	sessions map[string]struct{}
}

// newUpstreamDiag builds the allowlist from a comma-separated list of Claude
// Code session IDs. An empty list disables diagnostics.
func newUpstreamDiag(sessionIDs []string) *upstreamDiag {
	d := &upstreamDiag{sessions: make(map[string]struct{})}
	for _, id := range sessionIDs {
		if id = strings.TrimSpace(id); id != "" {
			d.sessions[id] = struct{}{}
		}
	}
	return d
}

func (d *upstreamDiag) enabledFor(sessionID string) bool {
	if d == nil || len(d.sessions) == 0 || sessionID == "" {
		return false
	}
	_, ok := d.sessions[sessionID]
	return ok
}

// requestShape summarizes a Kiro payload without any content.
type requestShape struct {
	historyUser      int
	historyAssistant int
	historyToolUses  int
	historyTextBytes int
	currentTextBytes int
	currentTools     int
	currentToolRes   int
	currentToolIDs   int
	toolResultBlocks int
	toolResultJSON   int
	toolResultText   int
	toolResultBytes  int
}

func summarizeShape(p *kiroproto.Payload) requestShape {
	var s requestShape
	cs := p.ConversationState
	for _, h := range cs.History {
		if h.UserInputMessage != nil {
			s.historyUser++
			s.historyTextBytes += len(h.UserInputMessage.Content)
			addContextShape(&s, h.UserInputMessage.UserInputMessageContext)
		}
		if h.AssistantResponseMessage != nil {
			s.historyAssistant++
			s.historyTextBytes += len(h.AssistantResponseMessage.Content)
			s.historyToolUses += len(h.AssistantResponseMessage.ToolUses)
		}
	}
	cur := cs.CurrentMessage.UserInputMessage
	s.currentTextBytes = len(cur.Content)
	addContextShape(&s, cur.UserInputMessageContext)
	return s
}

func addContextShape(s *requestShape, ctx *kiroproto.UserInputMessageContext) {
	if ctx == nil {
		return
	}
	s.currentTools += len(ctx.Tools)
	s.currentToolRes += len(ctx.ToolResults)
	for _, tr := range ctx.ToolResults {
		s.currentToolIDs++
		s.toolResultBlocks += len(tr.Content)
		for _, c := range tr.Content {
			if c.JSON != nil {
				s.toolResultJSON++
			}
			if c.Text != "" {
				s.toolResultText++
				s.toolResultBytes += len(c.Text)
			}
		}
	}
}

// logRequest emits one diagnostic line for an upstream attempt. body must be
// the exact bytes passed to the upstream client.
func (d *upstreamDiag) logRequest(ctx context.Context, sessionID string, attempt int, body []byte, p *kiroproto.Payload) {
	if !d.enabledFor(sessionID) {
		return
	}
	sum := sha256.Sum256(body)
	s := summarizeShape(p)
	_, short := logging.TraceIDs(ctx)
	slog.InfoContext(ctx, "upstream request diagnostics",
		"trace_id", short,
		"session_id", logging.ShortID(sessionID),
		"attempt", attempt,
		"payload_bytes", len(body),
		"payload_sha256", hex.EncodeToString(sum[:]),
		"conversation_id_set", p.ConversationState.ConversationID != "",
		"history_user", s.historyUser,
		"history_assistant", s.historyAssistant,
		"history_tool_uses", s.historyToolUses,
		"history_text_bytes", s.historyTextBytes,
		"current_text_bytes", s.currentTextBytes,
		"current_tools", s.currentTools,
		"current_tool_results", s.currentToolRes,
		"current_tool_result_ids", s.currentToolIDs,
		"tool_result_blocks", s.toolResultBlocks,
		"tool_result_json_blocks", s.toolResultJSON,
		"tool_result_text_blocks", s.toolResultText,
		"tool_result_text_bytes", s.toolResultBytes,
	)
}
