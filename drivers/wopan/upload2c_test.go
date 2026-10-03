package template

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/wopan-sdk-go"
)

const uploadOK = `{"code":"0000","data":{"fid":"FID"},"msg":"ok"}`

// receivedPart is one upload2C request as seen by the fake WoPan upload server.
type receivedPart struct {
	uniqueID  string
	partIndex int64
	partSize  int64
	totalPart int64
	fileSize  int64
	fileName  string
	body      []byte
}

// fakeWoPan stands in for the WoPan upload endpoint. respond decides the reply
// for a given attempt (1-based) at a given part.
type fakeWoPan struct {
	*httptest.Server

	mu       sync.Mutex
	received []receivedPart
	attempts map[string]int
}

func newFakeWoPan(t *testing.T, respond func(attempt int, p receivedPart) (int, string)) *fakeWoPan {
	t.Helper()
	f := &fakeWoPan{attempts: map[string]int{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The handler runs on the server goroutine: never call t.Fatal here.
		if err := r.ParseMultipartForm(4 << 20); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()
		body, err := io.ReadAll(file)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		p := receivedPart{
			uniqueID:  r.FormValue("uniqueId"),
			partIndex: parseIntField(r.FormValue("partIndex")),
			partSize:  parseIntField(r.FormValue("partSize")),
			totalPart: parseIntField(r.FormValue("totalPart")),
			fileSize:  parseIntField(r.FormValue("fileSize")),
			fileName:  r.FormValue("fileName"),
			body:      body,
		}
		status, respBody := respond(f.record(p), p)
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, respBody)
	}))
	t.Cleanup(f.Close)
	return f
}

func parseIntField(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func (f *fakeWoPan) record(p receivedPart) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, p)
	key := p.uniqueID + "|" + strconv.FormatInt(p.partIndex, 10)
	f.attempts[key]++
	return f.attempts[key]
}

func (f *fakeWoPan) parts() []receivedPart {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]receivedPart(nil), f.received...)
}

// sessions reassembles the parts the server received, per upload session.
func (f *fakeWoPan) sessions() map[string][]byte {
	byID := map[string]map[int64][]byte{}
	for _, p := range f.parts() {
		if byID[p.uniqueID] == nil {
			byID[p.uniqueID] = map[int64][]byte{}
		}
		byID[p.uniqueID][p.partIndex] = p.body
	}
	out := map[string][]byte{}
	for id, parts := range byID {
		idx := make([]int64, 0, len(parts))
		for i := range parts {
			idx = append(idx, i)
		}
		sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
		var buf bytes.Buffer
		for _, i := range idx {
			buf.Write(parts[i])
		}
		out[id] = buf.Bytes()
	}
	return out
}

// newTestWopan points a driver at the fake server. The access token must be at
// least 16 bytes or Crypto.SetAccessToken leaves the cipher key empty and
// EncryptParam fails, and a non-nil ClassifyRuleData keeps GetFileType from
// asking the real API for the classify rule.
func newTestWopan(t *testing.T, serverURL string) *Wopan {
	t.Helper()
	client := wopan.Default()
	client.SetAccessToken("0123456789abcdef")
	client.ZoneURL = serverURL
	client.ClassifyRuleData = &wopan.ClassifyRuleData{}
	client.SetHttpClient(&http.Client{Transport: &http.Transport{}})
	return &Wopan{client: client}
}

// shrinkParts makes uploads use tiny parts and near-zero retry delays. Tests in
// this package run sequentially, so mutating these package variables is safe.
func shrinkParts(t *testing.T, partSize int64) {
	t.Helper()
	oldSize, oldTries, oldDelay := woPanPartSize, woPanPartTries, woPanPartRetryDelay
	woPanPartSize = partSize
	woPanPartTries = 3
	woPanPartRetryDelay = time.Millisecond
	t.Cleanup(func() {
		woPanPartSize, woPanPartTries, woPanPartRetryDelay = oldSize, oldTries, oldDelay
	})
}

func testPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func uploadToFake(t *testing.T, d *Wopan, name string, payload []byte, ctx context.Context) (string, error) {
	t.Helper()
	if ctx == nil {
		ctx = context.Background()
	}
	return d.upload2C(wopan.SpaceTypePersonal, wopan.Upload2CFile{
		Name:        name,
		Size:        int64(len(payload)),
		Content:     bytes.NewReader(payload),
		ContentType: "application/octet-stream",
	}, "dir-id", "", wopan.Upload2COption{Ctx: ctx})
}

func TestUpload2CSendsEveryPart(t *testing.T) {
	shrinkParts(t, 1024)
	srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
		return http.StatusOK, fmt.Sprintf(`{"code":"0000","data":{"fid":"FID-%d"},"msg":"ok"}`, p.partIndex)
	})
	d := newTestWopan(t, srv.URL)

	// totalPart is size/partSize (floored), so an exact multiple is the only way
	// to get equally sized parts.
	payload := testPayload(3 * 1024)
	fid, err := uploadToFake(t, d, "movie.mp4", payload, nil)
	if err != nil {
		t.Fatalf("upload2C: %v", err)
	}
	if fid != "FID-3" {
		t.Fatalf("fid = %q, want the fid of the last part", fid)
	}

	parts := srv.parts()
	if len(parts) != 3 {
		t.Fatalf("got %d requests, want 3", len(parts))
	}
	for i, p := range parts {
		if p.partIndex != int64(i+1) {
			t.Errorf("request %d has partIndex %d, want %d", i, p.partIndex, i+1)
		}
		if p.partSize != 1024 {
			t.Errorf("part %d has partSize %d, want 1024", p.partIndex, p.partSize)
		}
		if p.totalPart != 3 {
			t.Errorf("part %d has totalPart %d, want 3", p.partIndex, p.totalPart)
		}
		if p.fileName != "movie.mp4" {
			t.Errorf("part %d has fileName %q", p.partIndex, p.fileName)
		}
		if p.fileSize != int64(len(payload)) {
			t.Errorf("part %d has fileSize %d, want %d", p.partIndex, p.fileSize, len(payload))
		}
	}

	sessions := srv.sessions()
	if len(sessions) != 1 {
		t.Fatalf("got %d upload sessions, want 1", len(sessions))
	}
	for id, assembled := range sessions {
		if !bytes.Equal(assembled, payload) {
			t.Fatalf("session %s reassembled to different bytes than the payload", id)
		}
	}
}

// A retried part must resend exactly the same bytes: file.Content is a one-way
// stream, so an implementation that re-reads it would upload the next part's
// bytes under the failed index.
func TestUpload2CRetriesPartWithSameBytes(t *testing.T) {
	shrinkParts(t, 1024)
	srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
		if p.partIndex == 2 && attempt == 1 {
			return http.StatusInternalServerError, `{"code":"9999","msg":"boom"}`
		}
		return http.StatusOK, uploadOK
	})
	d := newTestWopan(t, srv.URL)

	payload := testPayload(3 * 1024)
	if _, err := uploadToFake(t, d, "movie.mp4", payload, nil); err != nil {
		t.Fatalf("upload2C: %v", err)
	}

	parts := srv.parts()
	if len(parts) != 4 {
		t.Fatalf("got %d requests, want 4 (part 2 sent twice)", len(parts))
	}
	sessions := map[string]struct{}{}
	var part2 [][]byte
	for _, p := range parts {
		sessions[p.uniqueID] = struct{}{}
		if p.partIndex == 2 {
			part2 = append(part2, p.body)
		}
	}
	if len(part2) != 2 {
		t.Fatalf("part 2 was sent %d times, want 2", len(part2))
	}
	want := payload[1024:2048]
	for i, body := range part2 {
		if !bytes.Equal(body, want) {
			t.Fatalf("part 2 attempt %d carried different bytes than the first one", i+1)
		}
	}
	if len(sessions) != 1 {
		t.Fatalf("the retry used %d upload sessions, want 1", len(sessions))
	}
	for _, assembled := range srv.sessions() {
		if !bytes.Equal(assembled, payload) {
			t.Fatal("the retried session reassembled to different bytes than the payload")
		}
	}
}

func TestUpload2CGivesUpAfterRetries(t *testing.T) {
	shrinkParts(t, 1024)
	srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
		return http.StatusInternalServerError, `{"code":"9999","msg":"down"}`
	})
	d := newTestWopan(t, srv.URL)

	_, err := uploadToFake(t, d, "movie.mp4", testPayload(3*1024), nil)
	if err == nil {
		t.Fatal("upload2C succeeded although every attempt failed")
	}
	parts := srv.parts()
	if len(parts) != int(woPanPartTries) {
		t.Fatalf("got %d attempts, want %d", len(parts), woPanPartTries)
	}
	for _, p := range parts {
		if p.partIndex != 1 {
			t.Fatalf("part %d was requested; only the failing part may be retried", p.partIndex)
		}
	}
	if !strings.Contains(err.Error(), "partIndex: 1") {
		t.Fatalf("error %q does not mention the failing part", err)
	}
}

func TestUpload2CRetriesTransportFailures(t *testing.T) {
	shrinkParts(t, 1024)
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = conn.Close() // no response at all: a transport failure
	}))
	t.Cleanup(srv.Close)
	d := newTestWopan(t, srv.URL)

	if _, err := uploadToFake(t, d, "movie.mp4", testPayload(1024), nil); err == nil {
		t.Fatal("upload2C succeeded although every attempt was a transport failure")
	}
	if got := attempts.Load(); got != int64(woPanPartTries) {
		t.Fatalf("got %d attempts, want %d", got, woPanPartTries)
	}
}

func TestUpload2CDoesNotRetryPermanentFailures(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"http 400", http.StatusBadRequest, `{"code":"9999","msg":"bad request"}`},
		{"http 403", http.StatusForbidden, `{"code":"9999","msg":"forbidden"}`},
		{"business rejection", http.StatusOK, `{"code":"1001","msg":"quota exceeded"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shrinkParts(t, 1024)
			srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
				return tt.status, tt.body
			})
			d := newTestWopan(t, srv.URL)

			if _, err := uploadToFake(t, d, "movie.mp4", testPayload(2*1024), nil); err == nil {
				t.Fatal("upload2C accepted a permanent failure")
			}
			if got := len(srv.parts()); got != 1 {
				t.Fatalf("got %d attempts, want 1: a permanent failure must not be retried", got)
			}
		})
	}
}

func TestUpload2CStopsRetryingWhenCanceled(t *testing.T) {
	shrinkParts(t, 1024)
	woPanPartRetryDelay = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var once sync.Once
	srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
		once.Do(cancel)
		return http.StatusInternalServerError, `{"code":"9999","msg":"down"}`
	})
	d := newTestWopan(t, srv.URL)

	_, err := uploadToFake(t, d, "movie.mp4", testPayload(2*1024), ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := len(srv.parts()); got != 1 {
		t.Fatalf("got %d attempts after the cancel, want 1", got)
	}
}

// A stream that ends early must fail the upload instead of sending a short part
// body, which the server could accept and store as a corrupt file.
func TestUpload2CRejectsTruncatedStream(t *testing.T) {
	shrinkParts(t, 1024)
	srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
		return http.StatusOK, uploadOK
	})
	d := newTestWopan(t, srv.URL)

	payload := testPayload(1500) // the stream claims 2500 bytes, i.e. two parts
	_, err := d.upload2C(wopan.SpaceTypePersonal, wopan.Upload2CFile{
		Name:        "movie.mp4",
		Size:        2500,
		Content:     bytes.NewReader(payload),
		ContentType: "application/octet-stream",
	}, "dir-id", "", wopan.Upload2COption{Ctx: context.Background()})
	if err == nil {
		t.Fatal("upload2C accepted a truncated stream")
	}
	if !errors.Is(err, errs.StreamIncomplete) {
		t.Fatalf("error %v does not wrap errs.StreamIncomplete", err)
	}
	if got := len(srv.parts()); got != 1 {
		t.Fatalf("got %d requests, want 1 (only the complete part)", got)
	}
}

// Concurrent uploads must never share the part buffer: mixing part data between
// files is the same corruption the uniqueId fix had to remove on the server
// side, and a shared buffer would cause it on the client side.
func TestUpload2CConcurrentUploadsDoNotMixParts(t *testing.T) {
	shrinkParts(t, 512)
	const uploads = 8

	srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
		// Make every session retry its second part, so uploads interleave.
		if p.partIndex == 2 && attempt == 1 {
			return http.StatusInternalServerError, `{"code":"9999","msg":"boom"}`
		}
		return http.StatusOK, uploadOK
	})
	d := newTestWopan(t, srv.URL)

	payloads := make([][]byte, uploads)
	for i := range payloads {
		payloads[i] = bytes.Repeat([]byte{byte('A' + i)}, 512*3+7)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, uploads)
	for i := range payloads {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := uploadToFake(t, d, fmt.Sprintf("file-%d.mp4", i), payloads[i], nil)
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent upload2C: %v", err)
		}
	}

	sessions := srv.sessions()
	if len(sessions) != uploads {
		t.Fatalf("got %d upload sessions, want %d", len(sessions), uploads)
	}
	remaining := make([][]byte, len(payloads))
	copy(remaining, payloads)
	for id, got := range sessions {
		matched := -1
		for i, want := range remaining {
			if want != nil && bytes.Equal(got, want) {
				matched = i
				break
			}
		}
		if matched < 0 {
			t.Fatalf("session %s received bytes matching no uploaded file: parts were mixed between uploads", id)
		}
		remaining[matched] = nil
	}
	for i, left := range remaining {
		if left != nil {
			t.Fatalf("upload %d was not received intact", i)
		}
	}
}
