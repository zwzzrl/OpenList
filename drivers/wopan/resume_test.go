package template

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/wopan-sdk-go"
)

// rejectedPart emulates a WoPan answer that fails one part for as long as the
// returned flag is set. Part failures are permanent (4xx), so the call under test
// gives up instead of retrying the part, and the next call is the one that has to
// continue the session.
func rejectedPart(t *testing.T, part int64) (*fakeWoPan, *atomic.Bool) {
	t.Helper()
	var reject atomic.Bool
	reject.Store(true)
	srv := newFakeWoPan(t, func(attempt int, p receivedPart) (int, string) {
		if p.partIndex == part && reject.Load() {
			return http.StatusBadRequest, `{"code":"4000","msg":"rejected"}`
		}
		return http.StatusOK, uploadOK
	})
	return srv, &reject
}

func uploadContent(d *Wopan, payload []byte, resume *woPanResume) (string, int64, error) {
	return d.upload2C(wopan.SpaceTypePersonal, wopan.Upload2CFile{
		Name:        "movie.mp4",
		Size:        int64(len(payload)),
		Content:     bytes.NewReader(payload),
		ContentType: "application/octet-stream",
	}, "dir-id", "", wopan.Upload2COption{}, resume)
}

// putWithSession runs one Put of a stream that belongs to a client upload
// session, the way a multipart attempt does.
func putWithSession(t *testing.T, d *Wopan, sessionID, name string, payload []byte) error {
	t.Helper()
	file := &stream.FileStream{
		Obj:             &model.Object{Name: name, Size: int64(len(payload))},
		Reader:          bytes.NewReader(payload),
		UploadSessionID: sessionID,
	}
	return d.Put(context.Background(), &model.Object{ID: "dir-id", IsFolder: true}, file, func(float64) {})
}

func partIndexes(parts []receivedPart) []int64 {
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		out = append(out, p.partIndex)
	}
	return out
}

func TestUpload2CResumesFromTheCommittedOffset(t *testing.T) {
	shrinkParts(t, 1024)
	srv, reject := rejectedPart(t, 2)
	d := newTestWopan(t, srv.URL)
	payload := testPayload(3 * 1024)

	_, committed, err := uploadContent(d, payload, nil)
	if err == nil {
		t.Fatal("upload2C reported success although part 2 was rejected")
	}
	if committed != 1024 {
		t.Fatalf("committed = %d, want 1024: only part 1 was accepted", committed)
	}
	first := srv.parts()
	uid := first[0].uniqueID

	reject.Store(false)
	fid, committed, err := uploadContent(d, payload, &woPanResume{uniqueID: uid, committed: committed})
	if err != nil {
		t.Fatalf("resumed upload2C: %v", err)
	}
	if fid != "FID" {
		t.Fatalf("fid = %q, want the id of the committed file", fid)
	}
	if committed != int64(len(payload)) {
		t.Fatalf("committed = %d, want %d", committed, len(payload))
	}

	rest := srv.parts()[len(first):]
	if got := partIndexes(rest); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("resumed attempt sent parts %v, want [2 3]: part 1 is already stored", got)
	}
	for _, p := range rest {
		if p.uniqueID != uid {
			t.Fatalf("resumed part %d used uniqueId %q, want the session %q",
				p.partIndex, p.uniqueID, uid)
		}
	}
	if assembled := srv.sessions()[uid]; !bytes.Equal(assembled, payload) {
		t.Fatalf("the two attempts reassemble to %d bytes, want the %d byte payload",
			len(assembled), len(payload))
	}
}

// Only whole parts may be skipped: an offset inside a part belongs to a part that
// has to be re-sent from its start, otherwise the file would be shifted.
func TestUpload2CResumeOnlySkipsWholeParts(t *testing.T) {
	shrinkParts(t, 1024)
	srv, reject := rejectedPart(t, 2)
	d := newTestWopan(t, srv.URL)
	payload := testPayload(3 * 1024)

	_, committed, err := uploadContent(d, payload, nil)
	if err == nil {
		t.Fatal("upload2C reported success although part 2 was rejected")
	}
	uid := srv.parts()[0].uniqueID

	reject.Store(false)
	if _, _, err := uploadContent(d, payload, &woPanResume{uniqueID: uid, committed: committed + 500}); err != nil {
		t.Fatalf("resumed upload2C: %v", err)
	}
	if assembled := srv.sessions()[uid]; !bytes.Equal(assembled, payload) {
		t.Fatal("resuming from an offset inside a part did not reassemble the payload")
	}
}

func TestPutResumesAnInterruptedUpload(t *testing.T) {
	shrinkParts(t, 1024)
	srv, reject := rejectedPart(t, 2)
	d := newTestWopan(t, srv.URL)
	payload := testPayload(3 * 1024)

	if err := putWithSession(t, d, "upload-1", "movie.mp4", payload); err == nil {
		t.Fatal("first Put reported success although part 2 was rejected")
	}
	firstUID := srv.parts()[0].uniqueID
	firstLen := len(srv.parts())

	reject.Store(false)
	if err := putWithSession(t, d, "upload-1", "movie.mp4", payload); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	parts := srv.parts()
	ids := map[string]bool{}
	sentPart1 := 0
	for _, p := range parts {
		ids[p.uniqueID] = true
		if p.partIndex == 1 {
			sentPart1++
		}
	}
	if len(ids) != 1 {
		t.Fatalf("the retry used %d WoPan sessions, want 1: %v", len(ids), ids)
	}
	if sentPart1 != 1 {
		t.Fatalf("part 1 was uploaded %d times, want once: a retry must continue, not restart", sentPart1)
	}
	if got := partIndexes(parts[firstLen:]); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("the retry sent parts %v, want [2 3]", got)
	}
	if assembled := srv.sessions()[firstUID]; !bytes.Equal(assembled, payload) {
		t.Fatal("the retry did not reassemble the payload")
	}
}

func TestPutDoesNotResumeAnotherSessionsUpload(t *testing.T) {
	shrinkParts(t, 1024)
	srv, reject := rejectedPart(t, 2)
	d := newTestWopan(t, srv.URL)
	payload := testPayload(3 * 1024)

	if err := putWithSession(t, d, "upload-1", "movie.mp4", payload); err == nil {
		t.Fatal("first Put reported success although part 2 was rejected")
	}
	firstUID := srv.parts()[0].uniqueID
	firstLen := len(srv.parts())

	reject.Store(false)
	if err := putWithSession(t, d, "upload-2", "movie.mp4", payload); err != nil {
		t.Fatalf("Put of another session: %v", err)
	}

	other := srv.parts()[firstLen:]
	if got := partIndexes(other); len(got) != 3 || got[0] != 1 {
		t.Fatalf("another session sent parts %v, want all parts from 1", got)
	}
	if other[0].uniqueID == firstUID {
		t.Fatalf("another session continued the WoPan session of a different upload (%s)", firstUID)
	}
}

// A session WoPan accepted without committing cannot be continued, so the next
// attempt has to open a new one instead of resuming into nothing.
func TestPutStartsOverWhenWoPanNeverCommits(t *testing.T) {
	shrinkParts(t, 1024)
	srv := newNoFidWoPan(t)
	d := newTestWopan(t, srv.URL)
	payload := testPayload(2 * 1024)

	for i := 1; i <= 2; i++ {
		err := putWithSession(t, d, "upload-1", "movie.mp4", payload)
		if !errors.Is(err, errWoPanUploadNotCommitted) {
			t.Fatalf("Put %d error = %v, want errWoPanUploadNotCommitted", i, err)
		}
	}

	parts := srv.parts()
	if len(parts) != 4 {
		t.Fatalf("got %d requests, want 4: both attempts send every part", len(parts))
	}
	if parts[2].partIndex != 1 {
		t.Fatalf("the second attempt started at part %d, want 1", parts[2].partIndex)
	}
	if parts[2].uniqueID == parts[0].uniqueID {
		t.Fatal("the second attempt continued a session WoPan never committed")
	}
}

func TestPutDoesNotResumeAnExpiredUpload(t *testing.T) {
	shrinkParts(t, 1024)
	srv, reject := rejectedPart(t, 2)
	d := newTestWopan(t, srv.URL)
	payload := testPayload(3 * 1024)

	if err := putWithSession(t, d, "upload-1", "movie.mp4", payload); err == nil {
		t.Fatal("first Put reported success although part 2 was rejected")
	}
	firstLen := len(srv.parts())
	d.uploads.mu.Lock()
	for _, e := range d.uploads.entries {
		e.lastUsed = time.Now().Add(-woPanResumeTTL - time.Minute)
	}
	d.uploads.mu.Unlock()

	reject.Store(false)
	if err := putWithSession(t, d, "upload-1", "movie.mp4", payload); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if got := partIndexes(srv.parts()[firstLen:]); len(got) != 3 || got[0] != 1 {
		t.Fatalf("an expired session was resumed: parts %v, want all parts from 1", got)
	}
}

// A second attempt of the same session arriving while the first one runs must not
// share its WoPan session, and a session whose file changed is not continued.
func TestWoPanUploadCacheKeepsSessionsApartAndForgetsThem(t *testing.T) {
	var c woPanUploadCache

	first := c.begin("s", 100)
	if first.committed != 0 {
		t.Fatalf("first attempt committed = %d, want 0", first.committed)
	}
	c.finish("s", first, 0, errors.New("boom"))

	// A retry of the failed session continues it ...
	running := c.begin("s", 100)
	if running.uniqueID != first.uniqueID {
		t.Fatalf("retry session = %q, want the recorded %q", running.uniqueID, first.uniqueID)
	}
	// ... but an attempt arriving while it runs must not be handed the same one.
	concurrent := c.begin("s", 100)
	if concurrent.uniqueID == running.uniqueID {
		t.Fatal("an attempt that is already running had its WoPan session handed to another one")
	}
	c.finish("s", running, 0, errors.New("boom"))
	c.finish("s", concurrent, 0, errors.New("boom"))

	// Same client session, different file size: nothing to continue.
	other := c.begin("s", 200)
	if other.uniqueID == concurrent.uniqueID {
		t.Fatal("a session whose file changed was continued")
	}
	c.finish("s", other, 0, nil)
	if _, ok := c.entries["s"]; ok {
		t.Fatal("a successful upload must not leave resume state behind")
	}
}
