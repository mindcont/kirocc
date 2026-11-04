package archive

import (
	"bytes"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/d-kuro/kirocc/internal/anthropic"
)

func TestAppendNonStreamingExchange(t *testing.T) {
	root := filepath.Join(t.TempDir(), "archive")
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	req := &anthropic.Request{
		Model: "claude-sonnet-4",
		Messages: []anthropic.Message{
			{Role: "user", Content: anthropic.MessageContent{Text: "hello"}},
		},
	}
	body := []byte(`{"type":"message","role":"assistant","content":[{"type":"text","text":"hi"}]}`)
	if err := store.Append("laptop_1", "session-1", "trace-1", req, http.StatusOK, body, "application/json"); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(root, "*", "laptop_1", "session-1.jsonl"))
	if err != nil || len(files) != 1 {
		t.Fatalf("archive files = %v, err = %v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var got exchange
	if err := json.UnmarshalRead(strings.NewReader(strings.TrimSpace(string(data))), &got); err != nil {
		t.Fatal(err)
	}
	if got.DeviceID != "laptop_1" || got.SessionID != "session-1" || got.TraceID != "trace-1" {
		t.Fatalf("record metadata = %+v", got)
	}
	if len(got.UserMessages) != 1 || got.UserMessages[0].Content.Text != "hello" || string(got.Response) != string(body) {
		t.Fatalf("record content = users:%+v response:%s", got.UserMessages, got.Response)
	}
	info, err := os.Stat(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		t.Fatalf("archive file permissions = %o, want no group/other access", info.Mode().Perm())
	}
}

func TestAppendEveryUserMessageForEachRound(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "archive"))
	if err != nil {
		t.Fatal(err)
	}
	first := &anthropic.Request{Model: "m", Messages: []anthropic.Message{
		{Role: "user", Content: anthropic.MessageContent{Text: "prompt one"}},
	}}
	if err := store.Append("device", "multi-turn", "trace-1", first, http.StatusOK, []byte(`{"content":[{"type":"text","text":"answer one"}]}`), "application/json"); err != nil {
		t.Fatal(err)
	}
	second := &anthropic.Request{Model: "m", Messages: []anthropic.Message{
		{Role: "user", Content: anthropic.MessageContent{Text: "prompt one"}},
		{Role: "assistant", Content: anthropic.MessageContent{Text: "answer one"}},
		{Role: "user", Content: anthropic.MessageContent{Text: "tool result"}},
		{Role: "user", Content: anthropic.MessageContent{Text: "prompt two"}},
	}}
	if err := store.Append("device", "multi-turn", "trace-2", second, http.StatusOK, []byte(`{"content":[{"type":"text","text":"answer two"}]}`), "application/json"); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(store.root, "*", "device", "multi-turn.jsonl"))
	if err != nil || len(files) != 1 {
		t.Fatalf("archive files = %v, err = %v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("record count = %d, want two appended rounds", len(lines))
	}
	var firstRecord, secondRecord exchange
	if err := json.UnmarshalRead(strings.NewReader(lines[0]), &firstRecord); err != nil {
		t.Fatal(err)
	}
	if err := json.UnmarshalRead(strings.NewReader(lines[1]), &secondRecord); err != nil {
		t.Fatal(err)
	}
	if len(firstRecord.UserMessages) != 1 || firstRecord.UserMessages[0].Content.Text != "prompt one" {
		t.Fatalf("first round prompts = %+v", firstRecord.UserMessages)
	}
	if len(secondRecord.UserMessages) != 2 || secondRecord.UserMessages[0].Content.Text != "tool result" || secondRecord.UserMessages[1].Content.Text != "prompt two" {
		t.Fatalf("second round prompts = %+v", secondRecord.UserMessages)
	}
	if !strings.Contains(string(firstRecord.Response), "answer one") || !strings.Contains(string(secondRecord.Response), "answer two") {
		t.Fatalf("round responses = %s / %s", firstRecord.Response, secondRecord.Response)
	}
}

func TestAppendStreamingExchangeDecodesEvents(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "archive"))
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	req := &anthropic.Request{Model: "m", Stream: true, Messages: []anthropic.Message{{Role: "user", Content: anthropic.MessageContent{Text: "q"}}}}
	if err := store.Append("device", "session", "trace", req, http.StatusOK, body, "text/event-stream"); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(store.root, "*", "device", "session.jsonl"))
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var got exchange
	if err := json.UnmarshalRead(strings.NewReader(strings.TrimSpace(string(data))), &got); err != nil {
		t.Fatal(err)
	}
	var events []sseEvent
	if err := json.UnmarshalRead(bytes.NewReader(got.Response), &events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Event != "content_block_delta" || !strings.Contains(string(events[0].Data), "hello") {
		t.Fatalf("events = %+v", events)
	}
}

func TestAppendEncodesUnsafeSessionIDAndSkipsErrors(t *testing.T) {
	store, err := New(filepath.Join(t.TempDir(), "archive"))
	if err != nil {
		t.Fatal(err)
	}
	req := &anthropic.Request{Messages: []anthropic.Message{{Role: "user", Content: anthropic.MessageContent{Text: "q"}}}}
	body := []byte(`{"ok":true}`)
	if err := store.Append("device", "../../outside", "trace", req, http.StatusOK, body, "application/json"); err != nil {
		t.Fatal(err)
	}
	if err := store.Append("device", "bad", "trace", req, http.StatusBadGateway, body, "application/json"); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(store.root, "*", "device", "b64-*.jsonl"))
	if len(files) != 1 {
		t.Fatalf("unsafe session ID archive path = %v", files)
	}
}

func TestCaptureWriterCapturesAndFlushes(t *testing.T) {
	inner := httptest.NewRecorder()
	w := NewCaptureWriter(inner)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Write([]byte("data: {}\n\n"))
	w.Flush()
	body, err := w.CapturedBody()
	if err != nil {
		t.Fatal(err)
	}
	if w.StatusCode() != http.StatusOK || string(body) != "data: {}\n\n" {
		t.Fatalf("status=%d body=%q", w.StatusCode(), body)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if !inner.Flushed {
		t.Fatal("underlying response was not flushed")
	}
}

func TestCaptureWriterSpillsLargeResponsesAndCleansUp(t *testing.T) {
	inner := httptest.NewRecorder()
	w := NewCaptureWriter(inner)
	payload := bytes.Repeat([]byte("x"), captureMemoryThreshold+1)
	if n, err := w.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write = %d, %v", n, err)
	}
	if w.spillPath == "" {
		t.Fatal("large response was not spilled to a temporary file")
	}
	if info, err := os.Stat(w.spillPath); err != nil || info.Mode().Perm()&0077 != 0 {
		t.Fatalf("capture file permissions = %v, %v", info, err)
	}
	captured, err := w.CapturedBody()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(captured, payload) {
		t.Fatalf("captured %d bytes, want %d", len(captured), len(payload))
	}
	path := w.spillPath
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("capture temp file still exists: %v", err)
	}
}
