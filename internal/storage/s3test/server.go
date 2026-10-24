// Package s3test is a small in-process S3 endpoint for exercising the real
// S3 client (minio-go) without a network or Docker. It implements path-style
// PutObject (plain and aws-chunked bodies), multipart uploads, GetObject,
// HeadObject, DeleteObject and ListObjectsV2 for a single bucket.
//
// It checks that requests carry the configured access key id but does not
// verify signatures; signature correctness is covered by `make integration`
// against a real MinIO server.
package s3test

import (
	"bufio"
	"bytes"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 by definition.
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server is a fake S3 endpoint.
type Server struct {
	*httptest.Server
	Bucket      string
	AccessKeyID string

	mu       sync.Mutex
	objects  map[string][]byte
	uploads  map[string]map[int][]byte
	nextID   int
	requests map[string]int
}

// New starts a server for bucket accepting accessKeyID. Close it when done.
func New(bucket, accessKeyID string) *Server {
	s := &Server{
		Bucket:      bucket,
		AccessKeyID: accessKeyID,
		objects:     map[string][]byte{},
		uploads:     map[string]map[int][]byte{},
		requests:    map[string]int{},
	}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// Requests returns how many requests of a kind (e.g. "PutObject",
// "UploadPart") were served.
func (s *Server) Requests(kind string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[kind]
}

// Object returns a stored object's bytes.
func (s *Server) Object(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	return b, ok
}

type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3Error{Code: code, Message: msg})
}

func writeXML(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(v)
}

func etag(b []byte) string {
	sum := md5.Sum(b) //nolint:gosec // ETag.
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func (s *Server) count(kind string) {
	s.mu.Lock()
	s.requests[kind]++
	s.mu.Unlock()
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("Authorization"), "Credential="+s.AccessKeyID+"/") {
		writeError(w, http.StatusForbidden, "InvalidAccessKeyId", "unknown access key")
		return
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucket != s.Bucket {
		writeError(w, http.StatusNotFound, "NoSuchBucket", "bucket does not exist")
		return
	}
	q := r.URL.Query()
	if key == "" {
		s.serveBucket(w, r)
		return
	}
	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		s.initiate(w, key)
	case r.Method == http.MethodPut && q.Has("uploadId"):
		s.uploadPart(w, r, q.Get("uploadId"), q.Get("partNumber"))
	case r.Method == http.MethodPost && q.Has("uploadId"):
		s.complete(w, r, key, q.Get("uploadId"))
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		s.count("AbortMultipartUpload")
		s.mu.Lock()
		delete(s.uploads, q.Get("uploadId"))
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut:
		s.count("PutObject")
		body, err := readBody(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "IncompleteBody", err.Error())
			return
		}
		s.mu.Lock()
		s.objects[key] = body
		s.mu.Unlock()
		w.Header().Set("ETag", etag(body))
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		s.get(w, r, key)
	case r.Method == http.MethodDelete:
		s.count("DeleteObject")
		s.mu.Lock()
		delete(s.objects, key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method)
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, key string) {
	s.count("GetObject")
	s.mu.Lock()
	body, ok := s.objects[key]
	s.mu.Unlock()
	if !ok {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	w.Header().Set("ETag", etag(body))
	w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

type listResult struct {
	XMLName     xml.Name      `xml:"ListBucketResult"`
	Name        string        `xml:"Name"`
	Prefix      string        `xml:"Prefix"`
	KeyCount    int           `xml:"KeyCount"`
	MaxKeys     int           `xml:"MaxKeys"`
	IsTruncated bool          `xml:"IsTruncated"`
	Contents    []listContent `xml:"Contents"`
}

type listContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int    `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func (s *Server) serveBucket(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodHead:
		return
	case r.Method == http.MethodGet && q.Has("location"):
		writeXML(w, struct {
			XMLName xml.Name `xml:"LocationConstraint"`
			Region  string   `xml:",chardata"`
		}{Region: "us-east-1"})
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		s.count("ListObjectsV2")
		prefix := q.Get("prefix")
		res := listResult{Name: s.Bucket, Prefix: prefix, MaxKeys: 1000}
		s.mu.Lock()
		for k, v := range s.objects {
			if strings.HasPrefix(k, prefix) {
				res.Contents = append(res.Contents, listContent{
					Key: k, Size: len(v), ETag: etag(v), StorageClass: "STANDARD",
					LastModified: time.Unix(0, 0).UTC().Format(time.RFC3339),
				})
			}
		}
		s.mu.Unlock()
		sort.Slice(res.Contents, func(i, j int) bool { return res.Contents[i].Key < res.Contents[j].Key })
		res.KeyCount = len(res.Contents)
		writeXML(w, res)
	default:
		writeError(w, http.StatusNotImplemented, "NotImplemented", "bucket operation not supported")
	}
}

func (s *Server) initiate(w http.ResponseWriter, key string) {
	s.count("CreateMultipartUpload")
	s.mu.Lock()
	s.nextID++
	id := fmt.Sprintf("upload-%d", s.nextID)
	s.uploads[id] = map[int][]byte{}
	s.mu.Unlock()
	writeXML(w, struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{Bucket: s.Bucket, Key: key, UploadID: id})
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, id, part string) {
	s.count("UploadPart")
	n, err := strconv.Atoi(part)
	if err != nil || n < 1 {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "bad part number")
		return
	}
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "IncompleteBody", err.Error())
		return
	}
	s.mu.Lock()
	parts, ok := s.uploads[id]
	if ok {
		parts[n] = body
	}
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NoSuchUpload", id)
		return
	}
	w.Header().Set("ETag", etag(body))
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request, key, id string) {
	s.count("CompleteMultipartUpload")
	var req struct {
		Parts []struct {
			PartNumber int `xml:"PartNumber"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	s.mu.Lock()
	parts, ok := s.uploads[id]
	var buf bytes.Buffer
	missing := false
	for _, p := range req.Parts {
		b, have := parts[p.PartNumber]
		missing = missing || !have
		buf.Write(b)
	}
	if ok && !missing {
		s.objects[key] = buf.Bytes()
		delete(s.uploads, id)
	}
	s.mu.Unlock()
	if !ok || missing {
		writeError(w, http.StatusBadRequest, "InvalidPart", id)
		return
	}
	writeXML(w, struct {
		XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
		Bucket  string   `xml:"Bucket"`
		Key     string   `xml:"Key"`
		ETag    string   `xml:"ETag"`
	}{Bucket: s.Bucket, Key: key, ETag: etag(buf.Bytes())})
}

// readBody returns the request payload, decoding aws-chunked framing when the
// client used a streaming signature.
func readBody(r *http.Request) ([]byte, error) {
	if !strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
		return io.ReadAll(r.Body)
	}
	return DecodeAWSChunked(r.Body)
}

// DecodeAWSChunked strips aws-chunked framing ("<hex>[;ext]\r\n<data>\r\n",
// terminated by a zero-length chunk and optional trailers).
func DecodeAWSChunked(body io.Reader) ([]byte, error) {
	br := bufio.NewReader(body)
	var out bytes.Buffer
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("aws-chunked: chunk header: %w", err)
		}
		sizeHex, _, _ := strings.Cut(strings.TrimRight(line, "\r\n"), ";")
		size, err := strconv.ParseInt(sizeHex, 16, 64)
		if err != nil || size < 0 {
			return nil, fmt.Errorf("aws-chunked: bad chunk size %q", sizeHex)
		}
		if size == 0 {
			return out.Bytes(), nil
		}
		if _, err := io.CopyN(&out, br, size); err != nil {
			return nil, fmt.Errorf("aws-chunked: chunk body: %w", err)
		}
		crlf := make([]byte, 2)
		if _, err := io.ReadFull(br, crlf); err != nil || string(crlf) != "\r\n" {
			return nil, fmt.Errorf("aws-chunked: missing chunk terminator")
		}
	}
}
