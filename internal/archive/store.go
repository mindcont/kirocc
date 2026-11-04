package archive

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/d-kuro/kirocc/internal/anthropic"
)

const maxCapturedResponse = 32 << 20

var safeDeviceID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var safeSessionID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

const maxArchiveRecord = 2*maxCapturedResponse + 1<<20

// Store appends successful Anthropic request/response exchanges as JSONL.
// An empty root disables archiving.
type Store struct {
	root string
	mu   sync.Mutex
}

// New creates an archive store. The archive root and its children are private
// to the service account (0700 directories, 0600 files).
func New(root string) (*Store, error) {
	if root == "" {
		return nil, nil
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create archive root: %w", err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		return nil, fmt.Errorf("secure archive root: %w", err)
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve archive root: %w", err)
	}
	return &Store{root: absolute}, nil
}

type exchange struct {
	Timestamp   time.Time         `json:"timestamp"`
	TraceID     string            `json:"trace_id,omitempty"`
	DeviceID    string            `json:"device_id"`
	SessionID   string            `json:"session_id"`
	Model       string            `json:"model"`
	UserMessage anthropic.Message `json:"user_message"`
	Stream      bool              `json:"stream"`
	Response    jsontext.Value    `json:"assistant_response"`
}

// Append stores a successful response. The request context is reduced to its
// latest user message because clients resend the full conversation each turn.
func (s *Store) Append(deviceID, sessionID, traceID string, req *anthropic.Request, status int, response []byte, contentType string) error {
	if s == nil || s.root == "" || status != http.StatusOK {
		return nil
	}
	if !safeDeviceID.MatchString(deviceID) {
		return fmt.Errorf("invalid device ID for archive")
	}
	if len(sessionID) == 0 || len(sessionID) > 512 {
		return fmt.Errorf("invalid session ID length")
	}
	if req == nil || len(req.Messages) == 0 {
		return fmt.Errorf("missing request message")
	}
	responseJSON, err := encodeResponse(response, contentType)
	if err != nil {
		return fmt.Errorf("parse response for archive: %w", err)
	}
	userMessage := req.Messages[len(req.Messages)-1]
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			userMessage = req.Messages[i]
			break
		}
	}
	item := exchange{
		Timestamp:   time.Now().UTC(),
		TraceID:     traceID,
		DeviceID:    deviceID,
		SessionID:   sessionID,
		Model:       req.Model,
		UserMessage: userMessage,
		Stream:      req.Stream,
		Response:    responseJSON,
	}
	line, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("encode archive record: %w", err)
	}
	line = append(line, '\n')
	if len(line) > maxArchiveRecord {
		return fmt.Errorf("archive record exceeds size limit")
	}
	date := item.Timestamp.Format("2006-01-02")
	devicePath := filepath.Join(s.root, date, deviceID)
	if err := os.MkdirAll(devicePath, 0700); err != nil {
		return fmt.Errorf("create archive directory: %w", err)
	}
	if err := os.Chmod(filepath.Join(s.root, date), 0700); err != nil {
		return fmt.Errorf("secure archive date directory: %w", err)
	}
	if err := os.Chmod(devicePath, 0700); err != nil {
		return fmt.Errorf("secure archive device directory: %w", err)
	}
	path := filepath.Join(devicePath, safeSessionName(sessionID)+".jsonl")
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open archive file: %w", err)
	}
	_, writeErr := f.Write(line)
	closeErr := f.Close()
	if writeErr != nil {
		return fmt.Errorf("append archive record: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close archive file: %w", closeErr)
	}
	return nil
}

func safeSessionName(sessionID string) string {
	if len(sessionID) <= 128 && safeSessionID.MatchString(sessionID) {
		return sessionID
	}
	if len(sessionID) > 128 {
		sum := sha256.Sum256([]byte(sessionID))
		return "sha256-" + hex.EncodeToString(sum[:])
	}
	return "b64-" + base64.RawURLEncoding.EncodeToString([]byte(sessionID))
}

type sseEvent struct {
	Event string         `json:"event,omitempty"`
	Data  jsontext.Value `json:"data"`
}

func encodeResponse(body []byte, contentType string) (jsontext.Value, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, errors.New("empty response")
	}
	if strings.Contains(strings.ToLower(contentType), "text/event-stream") {
		events, err := decodeSSE(body)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(events)
		return encoded, err
	}
	if !jsontext.Value(body).IsValid() {
		return nil, errors.New("response is not valid JSON")
	}
	return append(jsontext.Value(nil), body...), nil
}

func decodeSSE(body []byte) ([]sseEvent, error) {
	var events []sseEvent
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 4096), maxCapturedResponse)
	var eventName string
	var data []string
	flush := func() error {
		if len(data) == 0 {
			eventName = ""
			return nil
		}
		payload := []byte(strings.Join(data, "\n"))
		if !jsontext.Value(payload).IsValid() {
			return errors.New("SSE event data is not valid JSON")
		}
		events = append(events, sseEvent{Event: eventName, Data: append(jsontext.Value(nil), payload...)})
		eventName = ""
		data = nil
		return nil
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := flush(); err != nil {
				return nil, err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			eventName = value
		case "data":
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, errors.New("SSE response has no data events")
	}
	return events, nil
}

// CaptureWriter tees an HTTP response to the client and retains a bounded copy
// for archival. It preserves flushing so SSE streaming remains live.
type CaptureWriter struct {
	http.ResponseWriter
	status   int
	body     bytes.Buffer
	tooLarge bool
}

func NewCaptureWriter(w http.ResponseWriter) *CaptureWriter {
	return &CaptureWriter{ResponseWriter: w}
}

func (w *CaptureWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *CaptureWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	if !w.tooLarge && n > 0 {
		if w.body.Len()+n > maxCapturedResponse {
			w.tooLarge = true
			w.body.Reset()
		} else {
			_, _ = w.body.Write(p[:n])
		}
	}
	return n, err
}

func (w *CaptureWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *CaptureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *CaptureWriter) StatusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *CaptureWriter) CapturedBody() []byte {
	if w.tooLarge {
		return nil
	}
	return append([]byte(nil), w.body.Bytes()...)
}

func (w *CaptureWriter) TooLarge() bool { return w.tooLarge }
