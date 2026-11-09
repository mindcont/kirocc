package messages

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/d-kuro/kirocc/internal/kiroproto"
	"github.com/d-kuro/kirocc/internal/logging"
)

func diagPayload() *kiroproto.Payload {
	return &kiroproto.Payload{
		ConversationState: kiroproto.ConversationState{
			ConversationID: "conv-1",
			History: []kiroproto.HistoryEntry{
				{UserInputMessage: &kiroproto.HistoryUserInputMessage{Content: "SECRET-PROMPT-TEXT"}},
				{AssistantResponseMessage: &kiroproto.AssistantResponseMessage{
					Content:  "assistant text",
					ToolUses: []kiroproto.HistoryToolUse{{ToolUseID: "t1", Name: "Bash"}},
				}},
			},
			CurrentMessage: kiroproto.CurrentMessage{UserInputMessage: kiroproto.UserInputMessage{
				Content: "current",
				UserInputMessageContext: &kiroproto.UserInputMessageContext{
					Tools: []kiroproto.ToolEntry{{CachePoint: &kiroproto.CachePoint{Type: "default"}}},
					ToolResults: []kiroproto.ToolResult{{
						ToolUseID: "t1",
						Status:    "success",
						Content: []kiroproto.ToolResultContent{
							{Text: "C:\\Users\\x\\file.go"},
							{JSON: map[string]any{"k": "v"}},
						},
					}},
				},
			}},
		},
	}
}

func TestNewUpstreamDiag_TrimsAndSkipsEmpty(t *testing.T) {
	d := newUpstreamDiag([]string{" a ", "", "  ", "b"})
	if len(d.sessions) != 2 {
		t.Fatalf("want 2 sessions, got %d", len(d.sessions))
	}
	if !d.enabledFor("a") || !d.enabledFor("b") {
		t.Error("expected trimmed IDs to be enabled")
	}
}

func TestUpstreamDiag_DisabledByDefault(t *testing.T) {
	var nilDiag *upstreamDiag
	if nilDiag.enabledFor("a") {
		t.Error("nil diag must be disabled")
	}
	if newUpstreamDiag(nil).enabledFor("a") {
		t.Error("empty allowlist must be disabled")
	}
}

func TestUpstreamDiag_OnlyAllowlistedSessions(t *testing.T) {
	d := newUpstreamDiag([]string{"allowed"})
	if d.enabledFor("other") {
		t.Error("session not in allowlist must be disabled")
	}
	if d.enabledFor("") {
		t.Error("empty session ID must be disabled")
	}
	if !d.enabledFor("allowed") {
		t.Error("allowlisted session must be enabled")
	}
}

func TestSummarizeShape(t *testing.T) {
	s := summarizeShape(diagPayload())
	want := requestShape{
		historyUser:      1,
		historyAssistant: 1,
		historyToolUses:  1,
		historyTextBytes: len("SECRET-PROMPT-TEXT") + len("assistant text"),
		currentTextBytes: len("current"),
		currentTools:     1,
		currentToolRes:   1,
		currentToolIDs:   1,
		toolResultBlocks: 2,
		toolResultJSON:   1,
		toolResultText:   1,
		toolResultBytes:  len("C:\\Users\\x\\file.go"),
	}
	if s != want {
		t.Errorf("shape mismatch:\n got  %+v\n want %+v", s, want)
	}
}

// The logged hash must cover the exact bytes the Kiro client sends. Both use
// encoding/json/v2 Marshal of the same payload, so serializing twice must match.
func TestUpstreamDiag_HashMatchesSerializedBody(t *testing.T) {
	p := diagPayload()
	body, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	again, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, again) {
		t.Fatal("payload serialization is not deterministic; hash would not be reproducible")
	}
	sum := sha256.Sum256(body)
	wantHash := hex.EncodeToString(sum[:])

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	ctx := logging.WithSessionID(context.Background(), "sess-abcdef12")
	newUpstreamDiag([]string{"sess-abcdef12"}).logRequest(ctx, "sess-abcdef12", 1, body, p)

	out := buf.String()
	if !strings.Contains(out, "payload_sha256="+wantHash) {
		t.Errorf("log missing expected hash %s:\n%s", wantHash, out)
	}
	if !strings.Contains(out, "payload_bytes="+strconv.Itoa(len(body))) {
		t.Errorf("log missing payload_bytes:\n%s", out)
	}
}

// Diagnostics must never contain prompt text, tool output or the session ID in full.
func TestUpstreamDiag_LogContainsNoContent(t *testing.T) {
	p := diagPayload()
	body, _ := json.Marshal(p)

	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	ctx := logging.WithSessionID(context.Background(), "sess-abcdef12")
	newUpstreamDiag([]string{"sess-abcdef12"}).logRequest(ctx, "sess-abcdef12", 1, body, p)

	out := buf.String()
	for _, forbidden := range []string{"SECRET-PROMPT-TEXT", "assistant text", "C:\\Users", "file.go", "sess-abcdef12"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("log leaked %q:\n%s", forbidden, out)
		}
	}
}

func TestUpstreamDiag_SkipsWhenNotEnabled(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	p := diagPayload()
	body, _ := json.Marshal(p)
	newUpstreamDiag([]string{"other"}).logRequest(context.Background(), "sess", 1, body, p)
	if buf.Len() != 0 {
		t.Errorf("expected no output for non-allowlisted session, got %q", buf.String())
	}
}
