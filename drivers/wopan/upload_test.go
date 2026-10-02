package template

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
)

func TestMaxWoPanFileSizeIsFAT32Limit(t *testing.T) {
	if want := int64(4*1024*1024*1024 - 1); maxWoPanFileSize != want {
		t.Fatalf("maxWoPanFileSize = %d, want %d", maxWoPanFileSize, want)
	}
}

func TestNextWoPanUploadIDMonotonic(t *testing.T) {
	prev, err := strconv.ParseInt(nextWoPanUploadID(), 10, 64)
	if err != nil {
		t.Fatalf("nextWoPanUploadID returned a non-numeric id: %v", err)
	}
	for i := 0; i < 1000; i++ {
		id, err := strconv.ParseInt(nextWoPanUploadID(), 10, 64)
		if err != nil {
			t.Fatalf("nextWoPanUploadID returned a non-numeric id: %v", err)
		}
		if id <= prev {
			t.Fatalf("id %d is not greater than the previous id %d", id, prev)
		}
		prev = id
	}
}

func TestNextWoPanUploadIDConcurrent(t *testing.T) {
	const (
		workers   = 32
		perWorker = 250
	)
	total := workers * perWorker

	ids := make(chan string, total)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				ids <- nextWoPanUploadID()
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, total)
	for id := range ids {
		if _, ok := seen[id]; ok {
			t.Fatalf("upload id %s was handed out more than once", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("got %d unique upload ids, want %d", len(seen), total)
	}
}

func TestValidateWoPanFileSize(t *testing.T) {
	tests := []struct {
		name    string
		size    int64
		wantErr bool
	}{
		{"zero size", 0, false},
		{"one byte", 1, false},
		{"one byte below limit", maxWoPanFileSize - 1, false},
		{"at limit", maxWoPanFileSize, false},
		{"one byte above limit", maxWoPanFileSize + 1, true},
		{"5 GiB", 5 * 1024 * 1024 * 1024, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWoPanFileSize("movie.mkv", tt.size)
			if tt.wantErr && err == nil {
				t.Fatalf("size %d was accepted, want an error", tt.size)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("size %d was rejected: %v", tt.size, err)
			}
		})
	}
}

func TestValidateWoPanFileSizeErrorMessage(t *testing.T) {
	err := validateWoPanFileSize("movie.mkv", maxWoPanFileSize+1)
	if err == nil {
		t.Fatal("an oversized file was accepted")
	}
	msg := err.Error()
	for _, want := range []string{"movie.mkv", strconv.FormatInt(maxWoPanFileSize, 10)} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q does not mention %q", msg, want)
		}
	}
}

// Put must reject an oversized file before it touches the WoPan client.
func TestPutRejectsOversizedFile(t *testing.T) {
	d := &Wopan{}
	file := &stream.FileStream{
		Obj:    &model.Object{Name: "movie.mkv", Size: maxWoPanFileSize + 1},
		Reader: bytes.NewReader(nil),
	}
	err := d.Put(context.Background(), &model.Object{ID: "0", IsFolder: true}, file, func(float64) {})
	if err == nil {
		t.Fatal("Put accepted a file above the WoPan size limit")
	}
	if !strings.Contains(err.Error(), "movie.mkv") {
		t.Fatalf("error %q does not mention the file name", err)
	}
}
