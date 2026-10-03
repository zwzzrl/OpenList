package multipart

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// Tests in this file share the putFile seam and must not run in parallel.

func setupSessionTest(t *testing.T) *Manager {
	t.Helper()
	oldConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() { conf.Conf = oldConf })
	return &Manager{byID: make(map[string]*Session), byKey: make(map[string]string)}
}

func stubPut(t *testing.T, fn func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error) {
	t.Helper()
	orig := putFile
	putFile = fn
	t.Cleanup(func() { putFile = orig })
}

func testUser() *model.User { return &model.User{ID: 7, Username: "tester"} }

func initReq(user *model.User, size, chunkSize int64) InitReq {
	return InitReq{
		User:      user,
		Path:      "/local/test.bin",
		Size:      size,
		ChunkSize: chunkSize,
		Mimetype:  "application/octet-stream",
		Modified:  time.Unix(1700000000, 0),
	}
}

func sendChunk(t *testing.T, m *Manager, user *model.User, id string, data []byte, idx int, chunkSize int64) SessionSnapshot {
	t.Helper()
	snap, err := m.Chunk(user, id, idx, bytes.NewReader(chunkOf(data, idx, chunkSize)))
	if err != nil {
		t.Fatalf("Chunk(%d): %v", idx, err)
	}
	return snap
}

func waitState(t *testing.T, m *Manager, user *model.User, id string, want State) SessionSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		snap, err := m.Status(user, id)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if snap.State == want {
			return snap
		}
		if time.Now().After(deadline) {
			t.Fatalf("session state = %s, want %s (err: %s)", snap.State, want, snap.Error)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSessionHappyPath(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(3*chunkSize + 300)
	data := genData(totalSize)
	user := testUser()

	got := make(chan []byte, 1)
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		if dst != "/local/" {
			return fmt.Errorf("unexpected dst dir %q", dst)
		}
		if fs.GetName() != "test.bin" || fs.GetSize() != totalSize {
			return fmt.Errorf("unexpected stream meta %s/%d", fs.GetName(), fs.GetSize())
		}
		b, err := io.ReadAll(fs)
		if err != nil {
			return err
		}
		up(100)
		got <- b
		return nil
	})

	snap, resumed, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil || resumed {
		t.Fatalf("Init = (resumed=%v, err=%v)", resumed, err)
	}
	if snap.TotalChunks != 4 || snap.State != StateReceiving {
		t.Fatalf("init snapshot = %+v", snap)
	}
	for _, idx := range []int{1, 0, 3, 2} { // out of order on purpose
		sendChunk(t, m, user, snap.ID, data, idx, chunkSize)
	}
	final, err := m.Complete(context.Background(), user, snap.ID)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if final.State != StateCompleted || final.StorageProgress != 100 {
		t.Fatalf("final snapshot = %+v", final)
	}
	if !bytes.Equal(<-got, data) {
		t.Fatal("driver received different bytes")
	}
	if _, err := m.Status(user, snap.ID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("session should be removed after Complete, got %v", err)
	}
}

func TestSessionRetriableRefill(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(2 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	attempts := 0
	got := make(chan []byte, 1)
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		attempts++
		if attempts == 1 {
			buf := make([]byte, chunkSize)
			if _, err := io.ReadFull(fs, buf); err != nil {
				return err
			}
			return errors.New("transient storage hiccup")
		}
		b, err := io.ReadAll(fs)
		if err != nil {
			return err
		}
		got <- b
		return nil
	})

	snap, _, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	sendChunk(t, m, user, snap.ID, data, 0, chunkSize)
	failed := waitState(t, m, user, snap.ID, StateFailedRetriable)
	if failed.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", failed.Attempt)
	}
	if len(failed.Received) != 0 {
		t.Fatalf("failed session must report nothing received, got %v", failed.Received)
	}

	// chunks other than 0 are rejected until the client restarts the fill
	if _, err := m.Chunk(user, snap.ID, 1, bytes.NewReader(chunkOf(data, 1, chunkSize))); err == nil {
		t.Fatal("chunk 1 on failed_retriable session: expected error")
	}
	// re-fill from chunk 0 respawns the pipeline
	sendChunk(t, m, user, snap.ID, data, 0, chunkSize)
	sendChunk(t, m, user, snap.ID, data, 1, chunkSize)
	final, err := m.Complete(context.Background(), user, snap.ID)
	if err != nil {
		t.Fatalf("Complete after refill: %v", err)
	}
	if final.State != StateCompleted || attempts != 2 {
		t.Fatalf("state=%s attempts=%d, want completed/2", final.State, attempts)
	}
	if !bytes.Equal(<-got, data) {
		t.Fatal("driver received different bytes after refill")
	}
}

func TestSessionPermanentFailure(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantErr string
	}{
		{
			name:    "permission denied",
			err:     fmt.Errorf("denied: %w", errs.PermissionDenied),
			wantErr: "denied: permission denied",
		},
		{
			name: "file above the storage size limit",
			err: errs.NewErr(errs.UploadLimitExceeded,
				"file %q size %d exceeds WoPan maximum file size of %d bytes (4 GiB)",
				"test.bin", int64(5)<<30, int64(4)<<30-1),
			wantErr: `upload limit exceeded; file "test.bin" size 5368709120 exceeds WoPan maximum file size of 4294967295 bytes (4 GiB)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := setupSessionTest(t)
			const chunkSize = 1024
			data := genData(chunkSize)
			user := testUser()

			putErr := tt.err
			stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
				defer fs.Close()
				return putErr
			})

			snap, _, err := m.Init(initReq(user, chunkSize, chunkSize))
			if err != nil {
				t.Fatalf("Init: %v", err)
			}
			failed := waitState(t, m, user, snap.ID, StateFailedPermanent)
			if failed.Attempt != 0 {
				t.Fatalf("attempt = %d, want 0: a permanent failure must not be retried", failed.Attempt)
			}
			// The client can only learn why the upload can never succeed from
			// the snapshot: the state alone says "do not retry", not "why".
			if !strings.Contains(failed.Error, tt.wantErr) {
				t.Fatalf("snapshot error = %q, want it to contain %q", failed.Error, tt.wantErr)
			}
			if _, err := m.Chunk(user, snap.ID, 0, bytes.NewReader(data)); err == nil {
				t.Fatal("chunk on failed_permanent session: expected error")
			}
			if _, err := m.Complete(context.Background(), user, snap.ID); err == nil {
				t.Fatal("Complete on failed_permanent session: expected error")
			}
		})
	}
}

// isPermanentPutError decides whether a failed pipeline attempt is retried or
// reported to the client as final. A storage that can never accept the file
// (e.g. it is above the driver's size limit) must be permanent, otherwise the
// client re-uploads the whole file on every retry.
func TestIsPermanentPutError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"transient network error", errors.New("connection reset by peer"), false},
		{"canceled", context.Canceled, false},
		{"wrapped canceled", fmt.Errorf("aborted: %w", context.Canceled), false},
		{"upload not supported", errs.UploadNotSupported, true},
		{"upload limit exceeded", errs.UploadLimitExceeded, true},
		{"wrapped upload limit exceeded", fmt.Errorf("Put: %w", errs.UploadLimitExceeded), true},
		{
			name: "wopan oversized file error",
			err: errs.NewErr(errs.UploadLimitExceeded,
				"file %q size %d exceeds WoPan maximum file size of %d bytes (4 GiB)",
				"movie.mkv", int64(5)<<30, int64(4)<<30-1),
			want: true,
		},
		{"permission denied", errs.PermissionDenied, true},
		{"storage not found", errs.StorageNotFound, true},
		{"object already exists", errs.ObjectAlreadyExists, true},
		{"relative path", errs.RelativePath, true},
		{"ignored system file", errs.IgnoredSystemFile, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPermanentPutError(tt.err); got != tt.want {
				t.Fatalf("isPermanentPutError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestRefillCRCMismatch(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(2 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	attempts := 0
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		attempts++
		if attempts == 1 {
			buf := make([]byte, chunkSize)
			if _, err := io.ReadFull(fs, buf); err != nil {
				return err
			}
			return errors.New("transient")
		}
		_, err := io.ReadAll(fs)
		return err
	})

	snap, _, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	sendChunk(t, m, user, snap.ID, data, 0, chunkSize)
	waitState(t, m, user, snap.ID, StateFailedRetriable)

	tampered := genData(chunkSize + 5)[:chunkSize] // different content, same length
	if _, err := m.Chunk(user, snap.ID, 0, bytes.NewReader(tampered)); err == nil {
		t.Fatal("re-fill with changed content: expected error")
	}
	waitState(t, m, user, snap.ID, StateFailedPermanent)
}

func TestCompleteRefusesIncomplete(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(3 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		_, err := io.ReadAll(fs)
		return err
	})

	snap, _, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	sendChunk(t, m, user, snap.ID, data, 0, chunkSize)
	if _, err := m.Complete(context.Background(), user, snap.ID); err == nil {
		t.Fatal("Complete with missing chunks: expected error")
	}
	// unblock the pipeline goroutine before the test tears down
	if err := m.Abort(user, snap.ID); err != nil {
		t.Fatalf("Abort: %v", err)
	}
}

func TestAbortAndOwnership(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(4 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		_, err := io.ReadAll(fs)
		return err
	})

	snap, _, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	sendChunk(t, m, user, snap.ID, data, 0, chunkSize)

	stranger := &model.User{ID: 99, Username: "stranger"}
	if _, err := m.Status(stranger, snap.ID); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("stranger Status err = %v, want ErrNotOwner", err)
	}
	if err := m.Abort(stranger, snap.ID); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("stranger Abort err = %v, want ErrNotOwner", err)
	}
	if err := m.Abort(user, snap.ID); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := m.Status(user, snap.ID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Status after Abort err = %v, want ErrSessionNotFound", err)
	}
}

func TestExpiry(t *testing.T) {
	m := setupSessionTest(t)
	m.ttl = 30 * time.Millisecond

	const chunkSize = 1024
	user := testUser()
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		_, err := io.ReadAll(fs)
		return err
	})

	snap, _, err := m.Init(initReq(user, 4*chunkSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	m.gc()
	if _, err := m.Status(user, snap.ID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Status after expiry err = %v, want ErrSessionNotFound", err)
	}
	// the terminated pipeline must release the ring file
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, _ := os.ReadDir(m.dir())
		if len(entries) == 0 {
			break
		}
		if time.Now().After(deadline) {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Fatalf("ring files not cleaned up after expiry: %v", names)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestInitResume(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(4 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		_, err := io.ReadAll(fs)
		return err
	})

	hashed := initReq(user, totalSize, chunkSize)
	hashed.Hashes = map[*utils.HashType]string{utils.MD5: "0123456789abcdef0123456789abcdef"}
	snap, _, err := m.Init(hashed)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	sendChunk(t, m, user, snap.ID, data, 2, chunkSize)

	// same hashes prove the same file: resume with buffered chunks skippable
	again, resumed, err := m.Init(hashed)
	if err != nil || !resumed {
		t.Fatalf("hashed re-Init = (resumed=%v, err=%v), want resumed", resumed, err)
	}
	if again.ID != snap.ID {
		t.Fatalf("resumed session id = %s, want %s", again.ID, snap.ID)
	}
	if len(again.Received) != 1 || again.Received[0] != [2]int{2, 2} {
		t.Fatalf("resumed received = %v, want [[2,2]]", again.Received)
	}

	// a different size is a different upload
	other, resumed, err := m.Init(initReq(user, totalSize+1, chunkSize))
	if err != nil || resumed || other.ID == snap.ID {
		t.Fatalf("different-size Init = (id=%s, resumed=%v, err=%v)", other.ID, resumed, err)
	}

	if _, err := m.Find(user, "/local/test.bin", totalSize); err != nil {
		t.Fatalf("Find: %v", err)
	}
	if _, err := m.Find(user, "/local/nope.bin", totalSize); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("Find miss err = %v, want ErrSessionNotFound", err)
	}

	// without hashes, path+size cannot prove identity against a receiving
	// session holding buffered data — the old session must be dropped, or a
	// same-sized different file would be silently mixed into the result
	bare := initReq(user, totalSize, chunkSize)
	fresh, resumed, err := m.Init(bare)
	if err != nil || resumed || fresh.ID == snap.ID {
		t.Fatalf("bare re-Init = (id=%s, resumed=%v, err=%v), want a fresh session", fresh.ID, resumed, err)
	}
	if _, err := m.Status(user, snap.ID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("superseded session err = %v, want ErrSessionNotFound", err)
	}
}

func TestResumeFailedRetriableWithoutHash(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(2 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	attempts := 0
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		attempts++
		if attempts == 1 {
			buf := make([]byte, chunkSize)
			if _, err := io.ReadFull(fs, buf); err != nil {
				return err
			}
			return errors.New("transient")
		}
		_, err := io.ReadAll(fs)
		return err
	})

	snap, _, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	sendChunk(t, m, user, snap.ID, data, 0, chunkSize)
	waitState(t, m, user, snap.ID, StateFailedRetriable)

	// a failed_retriable session keeps no chunk data, so identity proof is not
	// required to resume: the retry-button flow works without rapid hashing,
	// and the re-fill CRC check still catches a changed file
	again, resumed, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil || !resumed || again.ID != snap.ID {
		t.Fatalf("re-Init on failed_retriable = (id=%s, resumed=%v, err=%v), want resumed same session", again.ID, resumed, err)
	}
	sendChunk(t, m, user, snap.ID, data, 0, chunkSize)
	sendChunk(t, m, user, snap.ID, data, 1, chunkSize)
	if _, err := m.Complete(context.Background(), user, snap.ID); err != nil {
		t.Fatalf("Complete after hashless refill: %v", err)
	}
}

func TestRapidUploadShortCircuit(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(6 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	wantMD5 := "0123456789abcdef0123456789abcdef"
	wantSHA1 := "da39a3ee5e6b4b0d3255bfef95601890afd80709"
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		// the hash provided at init must reach the driver through the stream,
		// that is what lets PutRapid-style drivers skip the transfer entirely
		if got := fs.GetHash().GetHash(utils.MD5); got != wantMD5 {
			return fmt.Errorf("md5 not propagated: %q", got)
		}
		if got := fs.GetHash().GetHash(utils.SHA1); got != wantSHA1 {
			return fmt.Errorf("sha1 not propagated: %q", got)
		}
		time.Sleep(50 * time.Millisecond) // simulated rapid-upload API round trip
		up(100)
		return nil // rapid upload hit: succeed without reading the stream
	})

	req := initReq(user, totalSize, chunkSize)
	req.Hashes = map[*utils.HashType]string{utils.MD5: wantMD5, utils.SHA1: wantSHA1}
	snap, _, err := m.Init(req)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}

	// spam chunks across the completion moment: some land while receiving,
	// some race the window close, some arrive after completion — with the
	// completed-session absorption none of them may surface an error
	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for round := 0; round < 8; round++ {
				idx := (w*8 + round) % 6
				_, err := m.Chunk(user, snap.ID, idx, bytes.NewReader(chunkOf(data, idx, chunkSize)))
				// flow-control signals are part of the protocol, not failures
				if err != nil && !errors.Is(err, ErrChunkInFlight) && !errors.Is(err, ErrOutOfWindow) {
					errCh <- fmt.Errorf("worker %d chunk %d: %w", w, idx, err)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	final, err := m.Complete(context.Background(), user, snap.ID)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if final.State != StateCompleted || final.StorageProgress != 100 {
		t.Fatalf("final snapshot = %+v", final)
	}
}

// TestChunkRacesRapidCompletion pins the exact race the completed-session
// absorption exists for: a chunk request grabs the live window, stalls while
// receiving its body, the pipeline completes off the hash alone (rapid
// upload) and closes the window — the stalled chunk must then succeed
// idempotently instead of surfacing "window closed" to a client whose upload
// in fact just finished.
func TestChunkRacesRapidCompletion(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(2 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	proceed := make(chan struct{})
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		defer fs.Close()
		<-proceed // the rapid-upload verdict arrives when the test says so
		return nil
	})

	snap, _, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}

	gate := make(chan struct{})
	type result struct {
		snap SessionSnapshot
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		s, e := m.Chunk(user, snap.ID, 0,
			&gatedReader{release: gate, inner: bytes.NewReader(chunkOf(data, 0, chunkSize))})
		resCh <- result{s, e}
	}()

	// wait until the chunk writer holds slot 0 in the filling state
	m.mu.Lock()
	sess := m.byID[snap.ID]
	m.mu.Unlock()
	sess.mu.Lock()
	win := sess.win
	sess.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		win.mu.Lock()
		filling := win.slotState[0] == slotFilling
		win.mu.Unlock()
		if filling {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("chunk writer never reached the filling state")
		}
		time.Sleep(time.Millisecond)
	}

	close(proceed)                                 // rapid upload succeeds, window closes
	waitState(t, m, user, snap.ID, StateCompleted) // completion recorded
	close(gate)                                    // stalled chunk finishes against the closed window

	res := <-resCh
	if res.err != nil {
		t.Fatalf("in-flight chunk across rapid completion must be absorbed, got: %v", res.err)
	}
	if res.snap.State != StateCompleted {
		t.Fatalf("absorbed chunk snapshot state = %s, want completed", res.snap.State)
	}
}

// TestChunkDuringCompletionGap covers the moment between op.Put closing the
// window (its defer) and the verdict being recorded: a chunk hitting the
// closed window inside that gap must wait for the verdict and be absorbed,
// not bounce a "window closed" error at a client whose upload just succeeded.
func TestChunkDuringCompletionGap(t *testing.T) {
	m := setupSessionTest(t)
	const chunkSize = 1024
	totalSize := int64(2 * chunkSize)
	data := genData(totalSize)
	user := testUser()

	windowClosed := make(chan struct{})
	allowVerdict := make(chan struct{})
	stubPut(t, func(ctx context.Context, dst string, fs *stream.FileStream, up driver.UpdateProgress) error {
		_ = fs.Close() // what op.Put's defer does before Put returns
		close(windowClosed)
		<-allowVerdict // hold the pipeline return open: this IS the gap
		return nil
	})

	snap, _, err := m.Init(initReq(user, totalSize, chunkSize))
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	<-windowClosed

	type result struct {
		snap SessionSnapshot
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		s, e := m.Chunk(user, snap.ID, 0, bytes.NewReader(chunkOf(data, 0, chunkSize)))
		resCh <- result{s, e}
	}()

	select {
	case r := <-resCh:
		t.Fatalf("chunk inside the gap returned early with (%s, %v); it must wait for the verdict", r.snap.State, r.err)
	case <-time.After(150 * time.Millisecond):
		// still waiting on the verdict, as designed
	}

	close(allowVerdict)
	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("gap chunk must be absorbed after completion, got: %v", r.err)
		}
		if r.snap.State != StateCompleted {
			t.Fatalf("gap chunk snapshot state = %s, want completed", r.snap.State)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gap chunk never returned after the verdict")
	}
}

func TestInitRejectsBadSize(t *testing.T) {
	m := setupSessionTest(t)
	if _, _, err := m.Init(initReq(testUser(), 0, 1024)); err == nil {
		t.Fatal("Init with size 0: expected error")
	}
}
