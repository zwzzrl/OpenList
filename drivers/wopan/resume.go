package template

import (
	"errors"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
)

const (
	// woPanResumeTTL bounds how long an interrupted upload is worth continuing.
	// WoPan kept an unfinished session for at least 20 minutes of inactivity in
	// testing and multipart sessions expire after 30 minutes, so 15 minutes stays
	// inside the window the server is known to honour. Missing it only costs a
	// full re-upload, never a wrong file.
	woPanResumeTTL = 15 * time.Minute

	// woPanResumeMaxSessions bounds the cache. An entry is a few dozen bytes; the
	// cap only keeps a long-running process from accumulating one entry per
	// abandoned upload.
	woPanResumeMaxSessions = 512
)

// woPanResume is the WoPan session an upload continues: uniqueId is the session
// key the server stores parts under, and committed is the number of leading
// bytes it already holds.
type woPanResume struct {
	uniqueID  string
	committed int64
}

// uploadSessionID returns the id of the client upload session that feeds this
// stream, or "" when the upload has none (a plain fs/put, a task upload, ...)
// and therefore must never be resumed.
func uploadSessionID(file model.FileStreamer) string {
	if fs, ok := file.(*stream.FileStream); ok {
		return fs.UploadSessionID
	}
	return ""
}

// woPanUploadCache remembers the WoPan session of an interrupted upload per
// client upload session, so the next attempt of that session can send only the
// parts WoPan is missing.
//
// It belongs to one driver, and a driver belongs to one storage, while a
// multipart session belongs to one destination, so session ids cannot collide
// across storages. It lives in memory only, which is enough: the multipart
// sessions that refer to it do not survive an OpenList restart either.
type woPanUploadCache struct {
	mu      sync.Mutex
	entries map[string]*woPanUploadEntry
}

// woPanUploadEntry is one interrupted upload as the cache remembers it. inUse
// marks the attempt that currently owns the session: the multipart pipeline runs
// one attempt of a session at a time, so finding an entry in use means something
// unexpected, and that attempt gets a session of its own instead.
type woPanUploadEntry struct {
	uniqueID  string
	committed int64
	size      int64
	lastUsed  time.Time
	inUse     bool
}

// begin returns the state the attempt about to run should continue from, and
// marks the session as in use. It always returns a non-nil state for a non-empty
// session id: a fresh WoPan session when there is nothing usable to continue. The
// caller must pair every non-nil result with finish.
func (c *woPanUploadCache) begin(sessionID string, size int64) *woPanResume {
	if sessionID == "" {
		return nil
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]*woPanUploadEntry)
	}
	c.pruneLocked(now)

	e := c.entries[sessionID]
	if e == nil {
		e = &woPanUploadEntry{}
		c.entries[sessionID] = e
	}
	resume := &woPanResume{uniqueID: nextWoPanUploadID()}
	// Only a session whose file is still the same one may be continued, and only
	// when no other attempt is using it.
	if !e.inUse && e.uniqueID != "" && e.size == size {
		resume.uniqueID = e.uniqueID
		resume.committed = e.committed
	}
	e.size = size
	e.inUse = true
	e.lastUsed = now
	return resume
}

// finish records how much of the file WoPan holds after the attempt and releases
// the session. An upload WoPan never committed loses its state: continuing that
// session is exactly what did not work, so the next attempt must open a new one.
func (c *woPanUploadCache) finish(sessionID string, resume *woPanResume, committed int64, err error) {
	if sessionID == "" || resume == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[sessionID]
	if e == nil {
		return
	}
	e.inUse = false
	e.lastUsed = time.Now()
	if err == nil || errors.Is(err, errWoPanUploadNotCommitted) {
		delete(c.entries, sessionID)
		return
	}
	e.uniqueID = resume.uniqueID
	e.committed = committed
}

// pruneLocked drops expired entries and, when the cache is still at its cap, the
// least recently used ones. Callers must hold mu.
func (c *woPanUploadCache) pruneLocked(now time.Time) {
	for id, e := range c.entries {
		if !e.inUse && now.Sub(e.lastUsed) > woPanResumeTTL {
			delete(c.entries, id)
		}
	}
	for len(c.entries) >= woPanResumeMaxSessions {
		var oldestID string
		var oldest time.Time
		for id, e := range c.entries {
			if e.inUse {
				continue
			}
			if oldestID == "" || e.lastUsed.Before(oldest) {
				oldestID, oldest = id, e.lastUsed
			}
		}
		if oldestID == "" {
			return // every entry belongs to an attempt that is still running
		}
		delete(c.entries, oldestID)
	}
}
