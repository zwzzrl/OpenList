package template

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/wopan-sdk-go"
	"github.com/avast/retry-go"
)

// maxWoPanFileSize is the maximum file size supported by WoPan.
// It matches the FAT32 maximum single-file size: 4 GiB - 1 byte.
const maxWoPanFileSize = int64(4*1024*1024*1024 - 1)

var (
	woPanUploadIDMu   sync.Mutex
	woPanLastUploadID int64

	// woPanPartSize is the payload size of one upload2C part. It is a variable
	// only so tests can use tiny parts; production keeps the SDK default.
	woPanPartSize = wopan.DefaultPartSize
	// woPanPartTries bounds the attempts made for a single part, and
	// woPanPartRetryDelay is the delay before the second attempt (doubled after).
	// A part therefore blocks for at most woPanPartRetryDelay + 2*woPanPartRetryDelay
	// = 3s. The driver drains no part of the multipart ring while it waits, so a
	// concurrent chunk write may have to wait for a slot: 3s stays well inside
	// multipart.WindowWaitTimeout (10s), so a part retry cannot by itself push a
	// client chunk into ErrOutOfWindow.
	woPanPartTries      uint = 3
	woPanPartRetryDelay      = time.Second
)

// nextWoPanUploadID returns a process-wide unique, monotonic millisecond
// timestamp. WoPan uses the request's uniqueId as the upload session key. The
// upstream SDK calls time.Now().UnixMilli() directly, so concurrent uploads can
// receive the same id and overwrite each other's chunks.
func nextWoPanUploadID() string {
	woPanUploadIDMu.Lock()
	defer woPanUploadIDMu.Unlock()

	id := time.Now().UnixMilli()
	if id <= woPanLastUploadID {
		id = woPanLastUploadID + 1
	}
	woPanLastUploadID = id
	return strconv.FormatInt(id, 10)
}

// validateWoPanFileSize rejects files above the WoPan limit. The error wraps
// errs.UploadLimitExceeded so upload pipelines can tell an unsupported file
// size (permanent: retrying cannot succeed) apart from transient failures and
// stop re-uploading the whole file.
func validateWoPanFileSize(name string, size int64) error {
	if size > maxWoPanFileSize {
		return errs.NewErr(errs.UploadLimitExceeded,
			"file %q size %d exceeds WoPan maximum file size of %d bytes (4 GiB)",
			name, size, maxWoPanFileSize,
		)
	}
	return nil
}

// woPanPartError is one failed upload2C part attempt. It carries the HTTP status
// and the WoPan business code so the retry policy can tell a transient failure
// (network problem, 5xx, throttling) from a permanent one (bad request, expired
// token, business rejection) that resending the very same part cannot fix.
type woPanPartError struct {
	partIndex int64
	status    int    // HTTP status, 0 when no response was received
	code      string // WoPan business code, empty when there was none
	err       error
}

func (e *woPanPartError) Error() string { return e.err.Error() }

func (e *woPanPartError) Unwrap() error { return e.err }

// retryable reports whether another identical attempt could succeed.
func (e *woPanPartError) retryable() bool {
	switch {
	case errors.Is(e.err, context.Canceled), errors.Is(e.err, context.DeadlineExceeded):
		return false
	case e.status >= 500, e.status == http.StatusRequestTimeout, e.status == http.StatusTooManyRequests:
		return true
	case e.status >= 400:
		// 4xx: the request itself is wrong, sending it again changes nothing.
		return false
	case e.code != "":
		// The server answered but rejected the part: a business rule (quota,
		// session state, ...) that a retry does not change.
		return false
	default:
		// No response at all: a transport failure worth another attempt.
		return true
	}
}

func isRetryableWoPanPartError(err error) bool {
	var partErr *woPanPartError
	if errors.As(err, &partErr) {
		return partErr.retryable()
	}
	return true
}

// logWoPanPartFailure reports the final outcome of one part. retry-go calls
// OnRetry only for an attempt it is about to repeat, so a part that is rejected
// permanently (4xx or a business code) is never seen there: it is logged here,
// with the status or the code needed to spot a temporary one we misjudged as
// permanent. The attempt count separates a part that used up its retries (a real
// upload failure) from one the server refused outright.
func logWoPanPartFailure(name string, partIndex, totalPart int64, err error) {
	var partErr *woPanPartError
	if !errors.As(err, &partErr) {
		utils.Log.Warnf("wopan: upload part %d/%d of %q failed: %v", partIndex, totalPart, name, err)
		return
	}
	if partErr.retryable() {
		utils.Log.Warnf("wopan: giving up on upload part %d/%d of %q after %d attempts: %v",
			partIndex, totalPart, name, woPanPartTries, err)
		return
	}
	// An HTTP rejection carries no business code and a business rejection leaves
	// the status at 200, so exactly one of the two identifies the refusal.
	if partErr.code != "" {
		utils.Log.Warnf("wopan: upload part %d/%d of %q rejected permanently with business code %s: %v",
			partIndex, totalPart, name, partErr.code, err)
		return
	}
	utils.Log.Warnf("wopan: upload part %d/%d of %q rejected permanently with http status %d: %v",
		partIndex, totalPart, name, partErr.status, err)
}

// errWoPanUploadNotCommitted reports an upload every part of which WoPan
// accepted (code 0000) without ever returning the file id of a committed file.
// The server stored nothing, so the upload must be reported as failed instead
// of silently successful. It is deliberately not a permanent error: every
// upload2C call opens a new session under a new uniqueId, so the pipeline
// retrying the whole file is a real recovery path rather than a repeat of the
// same request.
var errWoPanUploadNotCommitted = errors.New("wopan: the server accepted every part but returned no file id, so no file was stored")

// upload2C is a local copy of wopan-sdk-go's Upload2C with three important
// differences:
//
//  1. uniqueId is generated by nextWoPanUploadID instead of
//     time.Now().UnixMilli(). The latter collides for concurrent uploads and
//     makes WoPan mix chunks from different files. The SDK does not expose a way
//     to pass a caller-provided uniqueId.
//  2. A failed part is retried within the same session instead of the whole file
//     being re-sent from part 1.
//  3. An upload WoPan accepted without ever returning the file id of a committed
//     file fails instead of being reported as a successful upload that stored
//     nothing.
//
// The session is fixed for the whole upload, not just for the retry: uniqueId,
// batchNo, the encrypted fileInfo (which carries batchNo), fileName and fileSize
// are all built once, before the part loop, and every attempt of every part
// sends them unchanged. Only partIndex and partSize differ between parts, and a
// retry repeats the failing part's values, so the retry writes the same bytes
// under the same key as the attempt it repeats.
func (d *Wopan) upload2C(spaceType string, file wopan.Upload2CFile, targetDirID string, familyID string, opt wopan.Upload2COption) (string, error) {
	client := d.client
	zoneURL := wopan.DefaultZoneURL
	if client.ZoneURL != "" {
		zoneURL = client.ZoneURL
	}

	ctx := opt.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	accessToken, _ := client.GetToken()
	batchNo := time.Now().Format("20060102150405")
	fileInfo := wopan.Json{
		"spaceType":   spaceType,
		"directoryId": targetDirID,
		"batchNo":     batchNo,
		"fileName":    file.Name,
		"fileSize":    file.Size,
		"fileType":    client.GetFileType(file.Name),
	}
	if spaceType == wopan.SpaceTypeFamily {
		fileInfo["familyId"] = familyID
	}
	fileInfoStr, err := client.EncryptParam(wopan.ChannelWoHome, fileInfo)
	if err != nil {
		return "", err
	}

	uploadURL := zoneURL + "/openapi/client/" + wopan.KeyUpload2C
	totalPart := file.Size / woPanPartSize
	if totalPart == 0 {
		totalPart = 1
	}
	formData := map[string]string{
		"uniqueId":    nextWoPanUploadID(),
		"accessToken": accessToken,
		"fileName":    file.Name,
		"psToken":     "undefined",
		"fileSize":    strconv.FormatInt(file.Size, 10),
		"totalPart":   strconv.FormatInt(totalPart, 10),
		"channel":     wopan.ChannelWoCloud,
		"directoryId": targetDirID,
		"fileInfo":    fileInfoStr,
	}

	// partBuf holds the part that is currently being sent, so that a retry can
	// resend exactly the same bytes: file.Content is a one-way stream (the
	// multipart window or a limited upload stream), and a retry cannot rewind
	// it. It is deliberately local to this call — a shared buffer would let
	// concurrent uploads overwrite each other's part data and mix chunks of
	// different files, the very failure mode uniqueId generation had to fix.
	//
	// Memory budget: one buffer per in-flight upload, partSize bytes (8 MiB for
	// WoPan) and up to 2*woPanPartSize, roughly 16 MiB, for the last part. How
	// many uploads run at once is not bounded here: tasks.upload.workers only
	// throttles task-style uploads, and browser chunk uploads bypass it, so
	// resident memory grows with the client's upload concurrency (roughly
	// N × 8-16 MiB). That is fine for the expected concurrency; a hard guard
	// would be a WoPan-wide upload semaphore rather than a temp-file fallback.
	// Until then, watch the per-part WARN lines from logWoPanPartFailure and the
	// process memory of a heavily concurrent browser upload.
	var partBuf []byte
	var fid string
	var finishedSize int64
	for partIndex := int64(1); partIndex <= totalPart; partIndex++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}

		partSize := woPanPartSize
		if partIndex == totalPart {
			partSize = file.Size - finishedSize
		}

		// Read the whole part before sending it. Besides making the attempt
		// retryable, this refuses to send a short body when the stream ends
		// early, which the server could accept as a corrupt upload.
		if int64(cap(partBuf)) < partSize {
			partBuf = make([]byte, partSize)
		}
		part := partBuf[:partSize]
		if _, err := io.ReadFull(file.Content, part); err != nil {
			return "", errs.NewErr(errs.StreamIncomplete,
				"partIndex: %d, failed to read %d bytes: %v", partIndex, partSize, err)
		}

		formData["partSize"] = strconv.FormatInt(partSize, 10)
		formData["partIndex"] = strconv.FormatInt(partIndex, 10)

		// partFid is what this part's attempt reported. WoPan answers every
		// accepted part with 0000 and attaches the file id only to the response
		// that commits the upload, so an empty value on the last part means the
		// server accepted the bytes without storing the file.
		var partFid string
		err := retry.Do(func() error {
			// resp must be fresh per attempt: reusing it would let a failed
			// attempt's parsed code/message leak into the next one.
			var resp wopan.Upload2CResp
			req := client.NewRequest().
				SetResult(&resp).
				ForceContentType("application/json;charset=UTF-8").
				SetHeaders(map[string]string{
					"Origin":     "https://pan.wo.cn",
					"Referer":    "https://pan.wo.cn/",
					"User-Agent": wopan.DefaultUA,
				}).
				SetMultipartFormData(formData).
				SetMultipartField("file", file.Name, file.ContentType, bytes.NewReader(part))
			req.SetContext(ctx)

			res, err := req.Post(uploadURL)
			if err != nil {
				return &woPanPartError{partIndex: partIndex, err: err}
			}
			if res.IsError() {
				return &woPanPartError{
					partIndex: partIndex,
					status:    res.StatusCode(),
					err: fmt.Errorf("partIndex: %d, failed to upload2C with http status: %d, body: %s",
						partIndex, res.StatusCode(), res.String()),
				}
			}
			if resp.Code != "0000" {
				return &woPanPartError{
					partIndex: partIndex,
					code:      resp.Code,
					err: fmt.Errorf("partIndex: %d, failed to upload2C with code: %s, msg: %s",
						partIndex, resp.Code, resp.Msg),
				}
			}
			partFid = resp.Data.Fid
			return nil
		},
			retry.Context(ctx),
			retry.Attempts(woPanPartTries),
			retry.DelayType(retry.BackOffDelay),
			retry.Delay(woPanPartRetryDelay),
			retry.LastErrorOnly(true),
			retry.RetryIf(isRetryableWoPanPartError),
			retry.OnRetry(func(n uint, err error) {
				// retry-go hands every retriable failure to this hook, including
				// the last attempt (where it gives up instead of repeating) and
				// one whose following delay returns at once because the context
				// is already done. Only announce the attempts that really happen.
				if n+1 >= woPanPartTries || ctx.Err() != nil {
					return
				}
				utils.Log.Debugf("wopan: upload part %d/%d of %q failed, retrying (attempt %d/%d): %v",
					partIndex, totalPart, file.Name, n+2, woPanPartTries, err)
			}),
		)
		// A canceled context is not a part failure: return it unwrapped so the
		// upload pipelines keep seeing the cancellation they expect.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		if err != nil {
			logWoPanPartFailure(file.Name, partIndex, totalPart, err)
			return "", err
		}

		// The last part is the one that commits the upload, and WoPan marks that
		// by returning the file id with it. Every part can answer 0000 while the
		// server stores nothing (for instance when it no longer has the session
		// the earlier parts belonged to), so an empty id here must fail the
		// upload: returning success would lose the file without a trace.
		if partIndex == totalPart {
			if partFid == "" {
				return "", errWoPanUploadNotCommitted
			}
			fid = partFid
		}

		finishedSize += partSize
		if opt.OnProgress != nil && file.Size > 0 {
			opt.OnProgress(finishedSize, file.Size)
		}
	}
	return fid, nil
}
