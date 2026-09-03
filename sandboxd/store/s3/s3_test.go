package s3

import (
	"bytes"
	"cmp"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/cocoonstack/sandbox/sandboxd/store"
	"github.com/cocoonstack/sandbox/sandboxd/store/storetest"
)

func TestS3BackendContract(t *testing.T) {
	fake := &fakeS3{objects: map[string][]byte{}}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	// Seekable bodies + when_required keep PutObject payloads plain (no
	// aws-chunked checksum trailers the fake would have to decode).
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	st, err := New(t.Context(), Config{
		Bucket:         "testbucket",
		Prefix:         "ck/",
		Endpoint:       ts.URL,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	storetest.RunContract(t, st)
}

// TestDeleteRetryConverges: a Delete retried after a partial failure re-deletes
// keys already gone; strict backends answer NoSuchKey per entry and the retry
// must still converge instead of failing forever.
func TestDeleteRetryConverges(t *testing.T) {
	const id = "ck_00000000000000bb"
	fake := &fakeS3{objects: map[string][]byte{
		"ck/" + id + "/" + store.MetaFile:                []byte(`{"id":"` + id + `"}`),
		"ck/" + id + "/" + store.ExportDir + "/disk.img": []byte("bytes"),
	}}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	st, err := New(t.Context(), Config{
		Bucket:         "testbucket",
		Prefix:         "ck/",
		Endpoint:       ts.URL,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := st.Delete(t.Context(), id); err != nil {
		t.Fatalf("delete retry after keys already gone: %v", err)
	}
	if len(fake.objects) != 0 {
		t.Errorf("objects left behind: %v", fake.objects)
	}
}

// TestFetchLegacyExportLayout: records published before per-generation
// export prefixes keep flat export/ keys; Fetch must fall back to them.
func TestFetchLegacyExportLayout(t *testing.T) {
	const id = "ck_00000000000000aa"
	fake := &fakeS3{objects: map[string][]byte{
		"ck/" + id + "/" + store.MetaFile:                []byte(`{"id":"` + id + `"}`),
		"ck/" + id + "/" + store.ExportDir + "/disk.img": []byte("legacy-bytes"),
	}}
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	st, err := New(t.Context(), Config{
		Bucket:         "testbucket",
		Prefix:         "ck/",
		Endpoint:       ts.URL,
		Region:         "us-east-1",
		ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dir, _, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer release()
	got, err := os.ReadFile(filepath.Join(dir, "disk.img")) //nolint:gosec // test path
	if err != nil || string(got) != "legacy-bytes" {
		t.Fatalf("fetched legacy export: %q, %v", got, err)
	}
}

func TestSparseCheckpointUploadsOnlyAllocatedExtents(t *testing.T) {
	const (
		id          = "ck_00000000000000cc"
		logicalSize = int64(128 << 20)
	)
	meta := []byte(`{"id":"` + id + `"}`)
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	st.sparse = true
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	export := filepath.Join(staging, store.ExportDir)
	if mkdirErr := os.MkdirAll(export, 0o750); mkdirErr != nil {
		t.Fatalf("mkdir export: %v", mkdirErr)
	}
	disk := filepath.Join(export, "disk.raw")
	f, err := os.OpenFile(disk, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		t.Fatalf("create sparse disk: %v", err)
	}
	if truncateErr := f.Truncate(logicalSize); truncateErr != nil {
		t.Fatalf("truncate sparse disk: %v", truncateErr)
	}
	markers := map[int64][]byte{
		4 << 10:                 []byte("first-extent"),
		logicalSize - (8 << 10): []byte("last-extent"),
	}
	for offset, marker := range markers {
		if _, writeErr := f.WriteAt(marker, offset); writeErr != nil {
			t.Fatalf("write sparse marker: %v", writeErr)
		}
	}
	if closeErr := f.Close(); closeErr != nil {
		t.Fatalf("close sparse disk: %v", closeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); writeErr != nil {
		t.Fatalf("write meta: %v", writeErr)
	}

	planned, _, sparse, err := planSparseFile(disk, "disk.raw")
	if err != nil {
		t.Fatalf("plan sparse file: %v", err)
	}
	if !sparse {
		t.Fatal("test filesystem did not expose sparse extents")
	}
	if planned.Size != logicalSize || len(planned.Chunks) == 0 {
		t.Fatalf("sparse plan = %+v", planned)
	}
	if publishErr := st.Publish(t.Context(), staging, id); publishErr != nil {
		t.Fatalf("Publish: %v", publishErr)
	}

	exportPrefix := "ck/" + id + "/" + exportGen(meta) + "/"
	fake.mu.Lock()
	objects := maps.Clone(fake.objects)
	fake.mu.Unlock()
	if _, ok := objects[exportPrefix+"disk.raw"]; ok {
		t.Fatal("sparse disk was uploaded as one logical-size object")
	}
	if _, ok := objects[exportPrefix+sparseManifestObject]; !ok {
		t.Fatal("sparse manifest was not uploaded")
	}
	var uploaded int64
	for key, body := range objects {
		if strings.HasPrefix(key, exportPrefix+sparseObjectPrefix+"/chunks/") {
			uploaded += int64(len(body))
		}
	}
	if uploaded == 0 || uploaded >= logicalSize/8 {
		t.Fatalf("uploaded sparse data = %d, logical size = %d", uploaded, logicalSize)
	}

	dir, _, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer release()
	restored := filepath.Join(dir, "disk.raw")
	info, err := os.Stat(restored)
	if err != nil {
		t.Fatalf("stat restored disk: %v", err)
	}
	if info.Size() != logicalSize || info.Mode().Perm() != 0o640 {
		t.Fatalf("restored disk size/mode = %d/%#o, want %d/%#o", info.Size(), info.Mode().Perm(), logicalSize, 0o640)
	}
	restoredFile, err := os.Open(restored) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("open restored disk: %v", err)
	}
	defer func() { _ = restoredFile.Close() }()
	for offset, marker := range markers {
		got := make([]byte, len(marker))
		if _, err := restoredFile.ReadAt(got, offset); err != nil {
			t.Fatalf("read marker at %d: %v", offset, err)
		}
		if !bytes.Equal(got, marker) {
			t.Errorf("marker at %d = %q, want %q", offset, got, marker)
		}
	}
	hole := make([]byte, 4096)
	if _, err := restoredFile.ReadAt(hole, logicalSize/2); err != nil {
		t.Fatalf("read restored hole: %v", err)
	}
	if !bytes.Equal(hole, make([]byte, len(hole))) {
		t.Fatal("restored hole contains non-zero data")
	}
}

func TestSparseCheckpointEncodingIsOptIn(t *testing.T) {
	const id = "ck_00000000000000cf"
	meta := []byte(`{"id":"` + id + `"}`)
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	export := filepath.Join(staging, store.ExportDir)
	if mkdirErr := os.MkdirAll(export, 0o750); mkdirErr != nil {
		t.Fatalf("mkdir export: %v", mkdirErr)
	}
	f, err := os.Create(filepath.Join(export, "disk.raw")) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("create sparse disk: %v", err)
	}
	if truncateErr := f.Truncate(1 << 20); truncateErr != nil {
		t.Fatalf("truncate sparse disk: %v", truncateErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		t.Fatalf("close sparse disk: %v", closeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); writeErr != nil {
		t.Fatalf("write meta: %v", writeErr)
	}
	if publishErr := st.Publish(t.Context(), staging, id); publishErr != nil {
		t.Fatalf("Publish: %v", publishErr)
	}
	key := "ck/" + id + "/" + exportGen(meta) + "/disk.raw"
	fake.mu.Lock()
	got := len(fake.objects[key])
	fake.mu.Unlock()
	if got != 1<<20 {
		t.Fatalf("default dense object size = %d, want %d", got, 1<<20)
	}
}

func TestSparseManifestRejectsMissingChunk(t *testing.T) {
	manifest := sparseManifest{
		Version: sparseManifestVersion,
		Files: []sparseFile{{
			Path: "disk.raw", Size: 4096,
			Chunks: []sparseChunk{{
				Size: 4096, Object: sparseObjectPrefix + "/chunks/file/0000",
				Extents: []sparseExtent{{Offset: 0, Size: 4096}},
			}},
		}},
	}
	err := validateSparseManifest(manifest, "ck/id/export/", []string{"ck/id/export/" + sparseManifestObject})
	if err == nil || !strings.Contains(err.Error(), "missing sparse chunk") {
		t.Fatalf("validate missing chunk = %v", err)
	}
}

func TestAllHoleCheckpointNeedsNoDataObject(t *testing.T) {
	const (
		id          = "ck_00000000000000cd"
		logicalSize = int64(32 << 20)
	)
	meta := []byte(`{"id":"` + id + `"}`)
	fake := &fakeS3{objects: map[string][]byte{}}
	st := newTestStore(t, fake)
	st.sparse = true
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	export := filepath.Join(staging, store.ExportDir)
	if mkdirErr := os.MkdirAll(export, 0o750); mkdirErr != nil {
		t.Fatalf("mkdir export: %v", mkdirErr)
	}
	disk := filepath.Join(export, "empty.raw")
	f, err := os.Create(disk) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("create sparse disk: %v", err)
	}
	if truncateErr := f.Truncate(logicalSize); truncateErr != nil {
		t.Fatalf("truncate sparse disk: %v", truncateErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		t.Fatalf("close sparse disk: %v", closeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); writeErr != nil {
		t.Fatalf("write meta: %v", writeErr)
	}
	if publishErr := st.Publish(t.Context(), staging, id); publishErr != nil {
		t.Fatalf("Publish: %v", publishErr)
	}

	exportPrefix := "ck/" + id + "/" + exportGen(meta) + "/"
	fake.mu.Lock()
	for key := range fake.objects {
		if strings.HasPrefix(key, exportPrefix+sparseObjectPrefix+"/chunks/") {
			fake.mu.Unlock()
			t.Fatalf("all-hole disk uploaded data object %s", key)
		}
	}
	fake.mu.Unlock()
	dir, _, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer release()
	info, err := os.Stat(filepath.Join(dir, "empty.raw"))
	if err != nil {
		t.Fatalf("stat restored all-hole disk: %v", err)
	}
	if info.Size() != logicalSize {
		t.Fatalf("restored all-hole disk size = %d, want %d", info.Size(), logicalSize)
	}
}

func TestExtentWriterMapsPackedObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.raw")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	defer func() { _ = f.Close() }()
	if truncateErr := f.Truncate(32); truncateErr != nil {
		t.Fatalf("truncate target: %v", truncateErr)
	}
	writer := newExtentWriterAt(f, []sparseExtent{{Offset: 3, Size: 4}, {Offset: 20, Size: 5}})
	if _, writeErr := writer.WriteAt([]byte("cdefghi"), 2); writeErr != nil {
		t.Fatalf("cross-extent WriteAt: %v", writeErr)
	}
	if _, writeErr := writer.WriteAt([]byte("ab"), 0); writeErr != nil {
		t.Fatalf("leading WriteAt: %v", writeErr)
	}
	got, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if string(got[3:7]) != "abcd" || string(got[20:25]) != "efghi" {
		t.Fatalf("mapped extents = %q/%q, want abcd/efghi", got[3:7], got[20:25])
	}
}

// TestS3BackendContractRealEndpoint runs the same contract against a real
// S3 implementation (MinIO on a testbed) when SANDBOX_S3_E2E names its
// endpoint — real list pagination, checksums, and path-style behavior the
// in-process fake cannot vouch for.
func TestS3BackendContractRealEndpoint(t *testing.T) {
	endpoint := os.Getenv("SANDBOX_S3_E2E")
	if endpoint == "" {
		t.Skip("SANDBOX_S3_E2E not set (export it to a MinIO endpoint to run)")
	}
	st, err := New(t.Context(), Config{
		Bucket:         cmp.Or(os.Getenv("SANDBOX_S3_E2E_BUCKET"), "sbx-checkpoints"),
		Prefix:         "contract/",
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
		Sparse:         true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	storetest.RunContract(t, st)
	t.Run("sparse multipart", func(t *testing.T) { runSparseRealEndpoint(t, st) })
}

func runSparseRealEndpoint(t *testing.T, st *Store) {
	t.Helper()
	const (
		id          = "ck_00000000000000ce"
		logicalSize = int64(96 << 20)
		dataOffset  = int64(8 << 20)
	)
	meta := []byte(`{"id":"` + id + `"}`)
	staging, err := st.Stage(id)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	export := filepath.Join(staging, store.ExportDir)
	if mkdirErr := os.MkdirAll(export, 0o750); mkdirErr != nil {
		t.Fatalf("mkdir export: %v", mkdirErr)
	}
	disk := filepath.Join(export, "disk.raw")
	f, err := os.OpenFile(disk, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("create sparse disk: %v", err)
	}
	if truncateErr := f.Truncate(logicalSize); truncateErr != nil {
		t.Fatalf("truncate sparse disk: %v", truncateErr)
	}
	data := bytes.Repeat([]byte{0x5a}, 20<<20) // exceeds multipart's 16 MiB part size.
	if _, writeErr := f.WriteAt(data, dataOffset); writeErr != nil {
		t.Fatalf("write sparse extent: %v", writeErr)
	}
	if closeErr := f.Close(); closeErr != nil {
		t.Fatalf("close sparse disk: %v", closeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(staging, store.MetaFile), meta, 0o600); writeErr != nil {
		t.Fatalf("write meta: %v", writeErr)
	}
	if publishErr := st.Publish(t.Context(), staging, id); publishErr != nil {
		t.Fatalf("Publish: %v", publishErr)
	}
	t.Cleanup(func() { _ = st.Delete(context.Background(), id) })

	exportPrefix := st.key(id, exportGen(meta)) + "/"
	keys, err := st.list(t.Context(), exportPrefix+sparseObjectPrefix+"/chunks/")
	if err != nil {
		t.Fatalf("list sparse chunks: %v", err)
	}
	var uploaded int64
	for _, key := range keys {
		head, headErr := st.client.HeadObject(t.Context(), &awss3.HeadObjectInput{Bucket: &st.bucket, Key: &key})
		if headErr != nil {
			t.Fatalf("head sparse chunk: %v", headErr)
		}
		uploaded += aws.ToInt64(head.ContentLength)
	}
	if uploaded < int64(len(data)) || uploaded >= logicalSize/2 {
		t.Fatalf("uploaded sparse bytes = %d, data/logical = %d/%d", uploaded, len(data), logicalSize)
	}
	dir, _, release, err := st.Fetch(t.Context(), id)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer release()
	restored, err := os.Open(filepath.Join(dir, "disk.raw")) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("open restored disk: %v", err)
	}
	defer func() { _ = restored.Close() }()
	probe := make([]byte, 4096)
	if _, err := restored.ReadAt(probe, dataOffset+(4<<20)); err != nil {
		t.Fatalf("read restored extent: %v", err)
	}
	if !bytes.Equal(probe, bytes.Repeat([]byte{0x5a}, len(probe))) {
		t.Fatal("restored multipart extent differs")
	}
}

func newTestStore(t *testing.T, fake *fakeS3) *Store {
	t.Helper()
	ts := httptest.NewServer(fake)
	t.Cleanup(ts.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
	st, err := New(t.Context(), Config{
		Bucket: "testbucket", Prefix: "ck/", Endpoint: ts.URL,
		Region: "us-east-1", ForcePathStyle: true,
	}, t.TempDir(), store.CheckpointIDRe)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return st
}

// fakeS3 implements just enough of the S3 REST surface (path-style) for
// the backend: PutObject, GetObject, DeleteObjects, ListObjectsV2.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte // key -> body
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := strings.TrimPrefix(r.URL.Path, "/testbucket/")
	switch {
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.objects[key] = body
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		prefix := r.URL.Query().Get("prefix")
		delim := r.URL.Query().Get("delimiter")
		type object struct {
			Key  string `xml:"Key"`
			Size int    `xml:"Size"`
		}
		type commonPrefix struct {
			Prefix string `xml:"Prefix"`
		}
		var result struct {
			XMLName        xml.Name `xml:"ListBucketResult"`
			IsTruncated    bool     `xml:"IsTruncated"`
			Contents       []object
			CommonPrefixes []commonPrefix
		}
		seen := map[string]bool{}
		for k, v := range f.objects {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			if delim != "" {
				if i := strings.Index(k[len(prefix):], delim); i >= 0 {
					cp := k[:len(prefix)+i+1]
					if !seen[cp] {
						seen[cp] = true
						result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix{Prefix: cp})
					}
					continue
				}
			}
			result.Contents = append(result.Contents, object{Key: k, Size: len(v)})
		}
		slices.SortFunc(result.Contents, func(a, b object) int { return cmp.Compare(a.Key, b.Key) })
		slices.SortFunc(result.CommonPrefixes, func(a, b commonPrefix) int { return cmp.Compare(a.Prefix, b.Prefix) })
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
	case r.Method == http.MethodHead:
		body, ok := f.objects[key]
		if !ok {
			http.Error(w, "NoSuchKey", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	case r.Method == http.MethodGet:
		body, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
			return
		}
		// transfermanager sizes ranged downloads via HeadObject then GETs
		// with a Range header; serve it so multi-part gets stay correct.
		if rng := r.Header.Get("Range"); rng != "" {
			var start, end int
			if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil && start < len(body) {
				if end >= len(body) {
					end = len(body) - 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(body[start : end+1])
				return
			}
		}
		_, _ = w.Write(body)
	case r.Method == http.MethodPost && r.URL.Query().Has("delete"):
		var req struct {
			Objects []struct {
				Key string `xml:"Key"`
			} `xml:"Object"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := xml.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad delete xml", http.StatusBadRequest)
			return
		}
		// Strict-backend emulation: absent keys answer a per-entry NoSuchKey
		// (AWS succeeds silently) so the client's tolerance stays exercised.
		type delErr struct {
			Key  string `xml:"Key"`
			Code string `xml:"Code"`
		}
		var result struct {
			XMLName xml.Name `xml:"DeleteResult"`
			Errors  []delErr `xml:"Error"`
		}
		for _, o := range req.Objects {
			if _, ok := f.objects[o.Key]; !ok {
				result.Errors = append(result.Errors, delErr{Key: o.Key, Code: "NoSuchKey"})
				continue
			}
			delete(f.objects, o.Key)
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
	case r.Method == http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unsupported", http.StatusNotImplemented)
	}
}
