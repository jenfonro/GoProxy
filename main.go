package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BuildVersion can be set at build time with:
//
//	go build -ldflags "-X main.BuildVersion=v1.0.0"
//
// When present and valid, it is returned from GET /version (as "v<semver>").
var BuildVersion string

// BuildCommit can be set at build time with:
//
//	go build -ldflags "-X main.BuildCommit=abcdef0"
//
// It is used for CI beta builds: GET /version => beta-<commit>.
var BuildCommit string

var localBetaStamp = strconv.FormatInt(time.Now().UnixMilli(), 10)

const (
	defaultListenHost = "0.0.0.0"
	defaultListenPort = 3010

	defaultTokenTTLSeconds = 3600

	defaultSpeedMaxBytes     = 4 * 1024 * 1024 * 1024
	defaultSpeedBytes        = 2 * 1024 * 1024
	defaultSpeedChunkBytes   = 64 * 1024
	defaultChunkThresholdB   = 64 * 1024 * 1024
	defaultUpstreamChunkSize = 256 * 1024
	defaultUpstreamPoolSize  = 10
	maxUpstreamPoolSize      = 32
	defaultChunkWindowBytes  = 16 * 1024 * 1024
	defaultUpstreamTimeoutMs = 10000
	defaultUpstreamRetries   = 1
	defaultProbeDirectConns  = 5
	maxProbeDirectConns      = 5
	defaultChunkHedgeConns   = 2
	maxChunkHedgeConns       = 5
	defaultChunkHedgeDelayMs = 500

	defaultConfigPollInterval = 1 * time.Second
	defaultConfigDebounce     = 200 * time.Millisecond
)

type streamOptions struct {
	ChunkSize        int64
	PoolSize         int
	Timeout          time.Duration
	Retries          int
	BypassChunking   bool
	ProbeDirectConns int
	ChunkHedgeConns  int
	ChunkHedgeDelay  time.Duration
}

func defaultStreamOptions() streamOptions {
	return streamOptions{
		ChunkSize:        int64(defaultUpstreamChunkSize),
		PoolSize:         defaultUpstreamPoolSize,
		Timeout:          time.Duration(defaultUpstreamTimeoutMs) * time.Millisecond,
		Retries:          defaultUpstreamRetries,
		BypassChunking:   false,
		ProbeDirectConns: defaultProbeDirectConns,
		ChunkHedgeConns:  defaultChunkHedgeConns,
		ChunkHedgeDelay:  time.Duration(defaultChunkHedgeDelayMs) * time.Millisecond,
	}
}

func normalizeStreamOptions(opts streamOptions) streamOptions {
	out := opts
	if out.ChunkSize <= 0 {
		out.ChunkSize = int64(defaultUpstreamChunkSize)
	}
	if out.PoolSize <= 0 {
		out.PoolSize = defaultUpstreamPoolSize
	}
	if out.PoolSize > maxUpstreamPoolSize {
		out.PoolSize = maxUpstreamPoolSize
	}
	if out.Timeout < 0 {
		out.Timeout = 0
	}
	if out.Timeout == 0 {
		out.Timeout = time.Duration(defaultUpstreamTimeoutMs) * time.Millisecond
	}
	if out.Retries < 0 {
		out.Retries = 0
	}
	if out.ProbeDirectConns <= 0 {
		out.ProbeDirectConns = defaultProbeDirectConns
	}
	if out.ProbeDirectConns > maxProbeDirectConns {
		out.ProbeDirectConns = maxProbeDirectConns
	}
	if out.ChunkHedgeConns <= 0 {
		out.ChunkHedgeConns = defaultChunkHedgeConns
	}
	if out.ChunkHedgeConns > maxChunkHedgeConns {
		out.ChunkHedgeConns = maxChunkHedgeConns
	}
	if out.ChunkHedgeDelay < 0 {
		out.ChunkHedgeDelay = 0
	}
	return out
}

type entry struct {
	URL                 string       `json:"url"`
	HeaderLines         []headerLine `json:"headers"`
	ContentType         string       `json:"contentType,omitempty"`
	FallbackContentType string       `json:"fallbackContentType,omitempty"`
	ProbeBypassDone     bool         `json:"-"`
	TS                  time.Time    `json:"-"`
}

type store struct {
	mu   sync.RWMutex
	ttl  time.Duration
	data map[string]*entry
}

func newStore(ttl time.Duration) *store {
	return &store{ttl: ttl, data: map[string]*entry{}}
}

func (s *store) prune() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range s.data {
		if v == nil || v.URL == "" || now.Sub(v.TS) > s.ttl {
			delete(s.data, k)
		}
	}
}

func (s *store) put(e *entry) string {
	token := randomToken(12)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	e.TS = now
	s.data[token] = e
	return token
}

func (s *store) get(token string) (*entry, bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[token]
	if !ok || e == nil || e.URL == "" {
		return nil, false
	}
	if now.Sub(e.TS) > s.ttl {
		delete(s.data, token)
		return nil, false
	}
	// Sliding expiration: refresh on access.
	e.TS = now
	return e, true
}

func (s *store) setDetectedContentType(token string, contentType string) {
	ct := strings.TrimSpace(contentType)
	if ct == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[token]
	if !ok || e == nil {
		return
	}
	if strings.TrimSpace(e.ContentType) == "" {
		e.ContentType = ct
	}
}

func (s *store) shouldBypassChunkForProbe(token string, rangeHeader string) bool {
	rh := strings.ToLower(strings.TrimSpace(rangeHeader))
	if rh != "bytes=0-" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[token]
	if !ok || e == nil {
		return false
	}
	if e.ProbeBypassDone {
		return false
	}
	e.ProbeBypassDone = true
	return true
}

func randomToken(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type headerLine struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func mapToHeaderLines(in map[string]string) []headerLine {
	if in == nil {
		return nil
	}
	out := make([]headerLine, 0, len(in))
	for k, v := range in {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(v)
		if key == "" || val == "" {
			continue
		}
		out = append(out, headerLine{Key: key, Value: val})
	}
	return out
}

func sanitizeHeaderLines(lines []headerLine) []headerLine {
	if len(lines) == 0 {
		return nil
	}
	out := make([]headerLine, 0, len(lines))
	for _, kv := range lines {
		key := strings.TrimSpace(kv.Key)
		val := strings.TrimSpace(kv.Value)
		if key == "" || val == "" {
			continue
		}
		out = append(out, headerLine{Key: key, Value: val})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func applyHeaderLines(req *http.Request, lines []headerLine) (hostOverride string) {
	if req == nil || len(lines) == 0 {
		return ""
	}
	for _, kv := range lines {
		k := strings.TrimSpace(kv.Key)
		v := strings.TrimSpace(kv.Value)
		if k == "" || v == "" {
			continue
		}
		// Host is special in net/http: it must be set via req.Host.
		if strings.EqualFold(k, "Host") {
			if hostOverride == "" {
				hostOverride = v
			}
			continue
		}
		req.Header.Add(k, v)
	}
	return hostOverride
}

func inferContentTypeFromURL(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	pickByName := func(name string) string {
		base := strings.TrimSpace(path.Base(name))
		if base == "" || base == "." || base == "/" {
			return ""
		}
		ext := strings.ToLower(strings.TrimSpace(path.Ext(base)))
		if ext == "" {
			return ""
		}
		switch ext {
		case ".mkv":
			return "video/x-matroska"
		case ".mp4":
			return "video/mp4"
		case ".m4v":
			return "video/x-m4v"
		case ".mov":
			return "video/quicktime"
		case ".webm":
			return "video/webm"
		case ".avi":
			return "video/x-msvideo"
		case ".ts", ".m2ts":
			return "video/mp2t"
		case ".flv":
			return "video/x-flv"
		}
		mt := strings.TrimSpace(mime.TypeByExtension(ext))
		if mt == "" {
			return ""
		}
		if i := strings.Index(mt, ";"); i >= 0 {
			mt = strings.TrimSpace(mt[:i])
		}
		if strings.HasPrefix(strings.ToLower(mt), "video/") {
			return mt
		}
		return ""
	}

	if v := strings.TrimSpace(u.Query().Get("response-content-disposition")); v != "" {
		for _, part := range strings.Split(v, ";") {
			p := strings.TrimSpace(part)
			lp := strings.ToLower(p)
			if strings.HasPrefix(lp, "filename*=") {
				val := strings.TrimSpace(p[len("filename*="):])
				val = strings.Trim(val, `"'`)
				if idx := strings.Index(strings.ToLower(val), "utf-8''"); idx >= 0 {
					val = val[idx+len("utf-8''"):]
				}
				if dec, err := url.QueryUnescape(val); err == nil {
					val = dec
				}
				if mt := pickByName(val); mt != "" {
					return mt
				}
			}
		}
		for _, part := range strings.Split(v, ";") {
			p := strings.TrimSpace(part)
			lp := strings.ToLower(p)
			if strings.HasPrefix(lp, "filename=") {
				val := strings.TrimSpace(p[len("filename="):])
				val = strings.Trim(val, `"'`)
				if dec, err := url.QueryUnescape(val); err == nil {
					val = dec
				}
				if mt := pickByName(val); mt != "" {
					return mt
				}
			}
		}
	}

	return pickByName(u.Path)
}

func normalizeContentType(raw string) string {
	ct := strings.TrimSpace(raw)
	if ct == "" {
		return ""
	}
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return strings.ToLower(ct)
}

func isVideoContentType(raw string) bool {
	ct := normalizeContentType(raw)
	return strings.HasPrefix(ct, "video/")
}

func chooseContentType(upstream string, fallback string) string {
	if isVideoContentType(upstream) {
		return normalizeContentType(upstream)
	}
	fb := normalizeContentType(fallback)
	if isVideoContentType(fb) {
		return fb
	}
	return ""
}

type repeatReader struct {
	p   []byte
	off int
}

func newRepeatReader(chunkSize int) *repeatReader {
	if chunkSize < 1024 {
		chunkSize = 1024
	}
	if chunkSize > 1024*1024 {
		chunkSize = 1024 * 1024
	}
	return &repeatReader{p: make([]byte, chunkSize), off: 0}
}

func (r *repeatReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	n := 0
	for n < len(dst) {
		remain := len(r.p) - r.off
		if remain <= 0 {
			r.off = 0
			remain = len(r.p)
		}
		toCopy := len(dst) - n
		if toCopy > remain {
			toCopy = remain
		}
		copy(dst[n:n+toCopy], r.p[r.off:r.off+toCopy])
		r.off += toCopy
		n += toCopy
	}
	return n, nil
}

type ctxWriter struct {
	ctx context.Context
	w   io.Writer
}

func (cw *ctxWriter) Write(p []byte) (int, error) {
	select {
	case <-cw.ctx.Done():
		return 0, cw.ctx.Err()
	default:
		return cw.w.Write(p)
	}
}

func serverVersion() string {
	semver := normalizeReleaseSemver(strings.TrimSpace(BuildVersion))
	if semver == "" {
		commit := strings.TrimSpace(BuildCommit)
		if commit != "" {
			return "beta-" + commit
		}
		return "beta-" + localBetaStamp
	}
	return "V" + semver
}

func normalizeReleaseSemver(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	s = strings.TrimPrefix(s, "refs/tags/")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	low := strings.ToLower(s)
	if low == "timestamp" || low == "beta" {
		return ""
	}

	// Accept "v1.2.3", "V1.2.3" and "1.2.3".
	if strings.HasPrefix(low, "v") {
		s = s[1:]
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// Very lightweight validation: ensure it starts with a digit.
	if s[0] < '0' || s[0] > '9' {
		return ""
	}
	return s
}

type config struct {
	BasePath string `json:"basePath"`
}

func defaultConfig() config {
	return config{BasePath: ""}
}

func normalizeBasePath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" || p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = strings.TrimRight(p, "/")
	if p == "/" {
		return ""
	}
	return p
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	if err != nil {
		return false
	}
	return st.Mode().IsRegular()
}

func findConfigPath() string {
	// Prefer config.json in current working directory, so `go run .` and typical service
	// deployments behave predictably.
	if fileExists("config.json") {
		return "config.json"
	}

	// If launched from the monorepo root, use GoProxy/config.json (create it if missing).
	if st, err := os.Stat("GoProxy"); err == nil && st.IsDir() {
		return filepath.Join("GoProxy", "config.json")
	}

	// Default: create config.json in current working directory.
	return "config.json"
}

func ensureDefaultConfigFile(path string) error {
	if fileExists(path) {
		return nil
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(defaultConfig(), "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func loadConfig(path string) (config, error) {
	f, err := os.Open(path)
	if err != nil {
		return config{}, err
	}
	defer f.Close()

	cfg := defaultConfig()
	dec := json.NewDecoder(f)
	if err := dec.Decode(&cfg); err != nil {
		return config{}, err
	}
	cfg.BasePath = normalizeBasePath(cfg.BasePath)
	return cfg, nil
}

func watchConfig(path string, initial config, stop <-chan struct{}, restart chan<- struct{}) {
	var lastMod time.Time
	if st, err := os.Stat(path); err == nil {
		lastMod = st.ModTime()
	}
	lastCfg := initial

	t := time.NewTicker(defaultConfigPollInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			st, err := os.Stat(path)
			if err != nil {
				continue
			}
			if !st.ModTime().After(lastMod) {
				continue
			}
			lastMod = st.ModTime()

			time.Sleep(defaultConfigDebounce)

			cfg, err := loadConfig(path)
			if err != nil {
				log.Printf("[config] reload failed: %v", err)
				continue
			}
			if cfg.BasePath == lastCfg.BasePath {
				continue
			}
			lastCfg = cfg
			log.Printf("[config] changed, restarting (basePath=%q)", cfg.BasePath)
			select {
			case restart <- struct{}{}:
			default:
			}
		}
	}
}

func tokenPrefix(basePath string) string {
	if basePath == "" {
		return "/"
	}
	return basePath + "/"
}

var errRestartRequested = errors.New("restart requested")

func serveOnce(
	client *http.Client,
	s *store,
	listen string,
	cfg config,
	restart <-chan struct{},
) error {
	basePath := cfg.BasePath

	speedMaxBytes := int64(defaultSpeedMaxBytes)
	speedDefaultBytes := int64(defaultSpeedBytes)
	speedChunkBytes := defaultSpeedChunkBytes

	mux := http.NewServeMux()

	// Reject unregistered routes with 403 (instead of falling back to net/http 404).
	// When basePath is set (e.g. "/proxy"), also reject the basePath root ("/proxy") directly
	// to avoid net/http's implicit redirect to "/proxy/".
	if basePath != "" {
		mux.HandleFunc(basePath, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions {
				writeCORSHeaders(w)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			writeCORSHeaders(w)
			http.Error(w, "Forbidden", http.StatusForbidden)
		})
	}

	// Client speed test endpoint: serves synthetic bytes to measure download throughput.
	// Example: GET /speed?bytes=2097152
	speedPath := mountPath(basePath, "/speed")
	mux.HandleFunc(speedPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		writeCORSHeaders(w)

		n := speedDefaultBytes
		if raw := strings.TrimSpace(r.URL.Query().Get("bytes")); raw != "" {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
				n = v
			}
		}
		if n < 0 {
			n = 0
		}
		if n > speedMaxBytes {
			n = speedMaxBytes
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodHead || n == 0 {
			return
		}

		reader := newRepeatReader(speedChunkBytes)
		_, _ = io.CopyN(&ctxWriter{ctx: r.Context(), w: w}, reader, n)
	})

	// TMDB API proxy: GET/HEAD /tmdb/<path> -> https://api.themoviedb.org/<path>
	//
	// This is intended to be used as MeowFilm's `tmdb_api_base`, e.g.:
	// - GoProxy basePath="/proxy"
	// - MeowFilm tmdb_api_base="https://example.com/proxy/tmdb/3"
	//
	// Notes:
	// - Keep the upstream host fixed to avoid SSRF.
	// - Register a longer prefix than tokenPathPrefix so net/http mux picks it.
	tmdbRootPath := mountPath(basePath, "/tmdb")
	tmdbPathPrefix := mountPath(basePath, "/tmdb/")
	mux.HandleFunc(tmdbRootPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeCORSHeaders(w)
		http.Error(w, "Forbidden", http.StatusForbidden)
	})
	mux.HandleFunc(tmdbPathPrefix, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		writeCORSHeaders(w)

		rest := strings.TrimPrefix(r.URL.Path, tmdbPathPrefix)
		rest = strings.TrimLeft(rest, "/")
		if rest == "" {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		up := &url.URL{
			Scheme:   "https",
			Host:     "api.themoviedb.org",
			Path:     "/" + rest,
			RawQuery: r.URL.RawQuery,
		}

		req, err := http.NewRequestWithContext(r.Context(), r.Method, up.String(), nil)
		if err != nil {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
			return
		}
		req.Header.Set("Accept", r.Header.Get("Accept"))
		if v := strings.TrimSpace(r.Header.Get("Authorization")); v != "" {
			req.Header.Set("Authorization", v)
		}
		if v := strings.TrimSpace(r.Header.Get("Accept-Language")); v != "" {
			req.Header.Set("Accept-Language", v)
		}
		if v := strings.TrimSpace(r.Header.Get("If-None-Match")); v != "" {
			req.Header.Set("If-None-Match", v)
		}
		if v := strings.TrimSpace(r.Header.Get("If-Modified-Since")); v != "" {
			req.Header.Set("If-Modified-Since", v)
		}

		upRes, err := client.Do(req)
		if err != nil {
			http.Error(w, "Bad Gateway", http.StatusBadGateway)
			return
		}
		defer upRes.Body.Close()

		copyHeader(w.Header(), upRes.Header)
		w.WriteHeader(upRes.StatusCode)
		if r.Method == http.MethodHead {
			return
		}
		_ = streamCopy(w, upRes.Body)
	})

	// TMDB image proxy: GET/HEAD /tmdb-img/<path> -> https://image.tmdb.org/<path>
	//
	// Example:
	// - GET /tmdb-img/t/p/w500/abc.jpg -> https://image.tmdb.org/t/p/w500/abc.jpg
	//
	// Notes:
	// - Keep the upstream host fixed to avoid SSRF.
	// - Register a longer prefix than tokenPathPrefix so net/http mux picks it.
	tmdbImgRootPath := mountPath(basePath, "/tmdb-img")
	tmdbImgPathPrefix := mountPath(basePath, "/tmdb-img/")
	mux.HandleFunc(tmdbImgRootPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeCORSHeaders(w)
		http.Error(w, "Forbidden", http.StatusForbidden)
	})
	mux.HandleFunc(tmdbImgPathPrefix, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		writeCORSHeaders(w)

		rest := strings.TrimPrefix(r.URL.Path, tmdbImgPathPrefix)
		rest = strings.TrimLeft(rest, "/")
		if rest == "" {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		up := &url.URL{
			Scheme:   "https",
			Host:     "image.tmdb.org",
			Path:     "/" + rest,
			RawQuery: r.URL.RawQuery,
		}

		// For images, support Range requests (some clients/CDNs like it).
		// Use the shared streaming proxy to reuse existing range/chunking logic.
		tw := &trackedWriter{ResponseWriter: w}
		if _, err := proxyStream(client, tw, r, up.String(), nil, "", "", "", r.Method == http.MethodHead, defaultStreamOptions()); err != nil {
			log.Printf("[tmdb-img] error=%v", err)
			if !tw.WroteHeader {
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
			}
			return
		}
	})

	// Proxy endpoint: GET/HEAD /<token> (or /<basePath>/<token>)
	// We intentionally use the root prefix so the public playback URL is compact.
	// Specific handlers like `/speed` and `/register` still win due to net/http mux longest-prefix matching.
	tokenPathPrefix := tokenPrefix(basePath)
	mux.HandleFunc(tokenPathPrefix, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		writeCORSHeaders(w)

		// Do not treat `/` (or the basePath root) as a token request.
		if r.URL.Path == tokenPathPrefix {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		raw := strings.TrimPrefix(r.URL.Path, tokenPathPrefix)
		raw = strings.Trim(raw, "/")
		if raw == "" {
			http.Error(w, "Missing token", http.StatusBadRequest)
			return
		}

		parts := strings.Split(raw, "/")
		seg0 := strings.TrimSpace(parts[0])

		// Do not allow paths that look like unregistered routes.
		// Example: /register/ or /speed/ should be rejected, not treated as a token.
		if seg0 == "register" || seg0 == "speed" || seg0 == "version" || seg0 == "tmdb" || seg0 == "tmdb-img" {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		if strings.Contains(raw, ".") || len(parts) > 2 {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		token := seg0
		action := ""
		if len(parts) >= 2 {
			action = strings.TrimSpace(parts[1])
		}

		e, ok := s.get(token)
		if !ok {
			http.Error(w, "Proxy token expired", http.StatusGone)
			return
		}

		target := strings.TrimSpace(e.URL)
		// Extended endpoints: /<token>/seg|key|pl?u=<upstream>
		// These are used by CatPawOpen m3u8 rewriting, where playlists stay in CatPawOpen
		// and binary payloads (segments/keys) are proxied by Go for performance.
		if action == "seg" || action == "key" || action == "pl" {
			writeCORSHeaders(w)
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
				return
			}
			uParam := strings.TrimSpace(r.URL.Query().Get("u"))
			if uParam == "" {
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}
			decoded, err := url.QueryUnescape(uParam)
			if err != nil || strings.TrimSpace(decoded) == "" {
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}
			target = strings.TrimSpace(decoded)
		}
		if target == "" {
			http.Error(w, "Invalid url", http.StatusBadRequest)
			return
		}
		u, err := url.Parse(target)
		if err != nil || u.Scheme == "" || u.Host == "" {
			http.Error(w, "Invalid url", http.StatusBadRequest)
			return
		}
		hostLower := strings.ToLower(strings.TrimSpace(u.Hostname()))
		if hostLower == "0.0.0.0" || hostLower == "127.0.0.1" || hostLower == "localhost" || hostLower == "::1" {
			http.Error(w, "Invalid url host", http.StatusBadRequest)
			return
		}
		if ip := net.ParseIP(hostLower); ip != nil && (ip.IsLoopback() || ip.IsPrivate()) {
			http.Error(w, "Invalid url host", http.StatusBadRequest)
			return
		}

		opts := defaultStreamOptions()
		if raw := strings.TrimSpace(r.URL.Query().Get("thread")); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil {
				opts.PoolSize = v
			}
		}
		// Keep compatibility with catpawrunner: chunkSize is in KB.
		if raw := strings.TrimSpace(r.URL.Query().Get("chunkSize")); raw != "" {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
				opts.ChunkSize = v * 1024
			}
		}
		// timeout is per-upstream-range-request timeout in milliseconds.
		if raw := strings.TrimSpace(r.URL.Query().Get("timeout")); raw != "" {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
				opts.Timeout = time.Duration(v) * time.Millisecond
			}
		}
		// probeConn controls first-probe direct connection parallelism.
		if raw := strings.TrimSpace(r.URL.Query().Get("probeConn")); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v > 0 {
				opts.ProbeDirectConns = v
			}
		}
		// chunkHedge controls per-chunk hedged request concurrency (1..3).
		if raw := strings.TrimSpace(r.URL.Query().Get("chunkHedge")); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil && v > 0 {
				opts.ChunkHedgeConns = v
			}
		}
		// chunkHedgeDelay controls delayed hedge trigger in milliseconds.
		if raw := strings.TrimSpace(r.URL.Query().Get("chunkHedgeDelay")); raw != "" {
			if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v >= 0 {
				opts.ChunkHedgeDelay = time.Duration(v) * time.Millisecond
			}
		}
		rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
		probePreflight := false
		if action == "" && s.shouldBypassChunkForProbe(token, rangeHeader) {
			opts.BypassChunking = true
			probePreflight = true
		}
		currentContentType := ""
		fallbackContentType := ""
		forcedContentType := ""
		if action == "" {
			currentContentType = strings.TrimSpace(e.ContentType)
			fallbackContentType = strings.TrimSpace(e.FallbackContentType)
		}
		tw := &trackedWriter{ResponseWriter: w}
		reqStart := time.Now()
		log.Printf("[proxy][start] token=%s action=%s method=%s range=%q probePreflight=%t probeConn=%d pool=%d chunk=%d timeoutMs=%d hedge=%d hedgeDelayMs=%d targetHost=%s",
			token,
			action,
			r.Method,
			rangeHeader,
			probePreflight,
			opts.ProbeDirectConns,
			opts.PoolSize,
			opts.ChunkSize,
			opts.Timeout.Milliseconds(),
			opts.ChunkHedgeConns,
			opts.ChunkHedgeDelay.Milliseconds(),
			u.Hostname(),
		)
		resolvedType, err := proxyStream(
			client,
			tw,
			r,
			target,
			e.HeaderLines,
			forcedContentType,
			currentContentType,
			fallbackContentType,
			r.Method == http.MethodHead,
			opts,
		)
		if err != nil {
			log.Printf("[proxy][error] token=%s action=%s method=%s range=%q probePreflight=%t status=%d wrote=%d durationMs=%d err=%v",
				token,
				action,
				r.Method,
				rangeHeader,
				probePreflight,
				tw.StatusCode,
				tw.BytesWritten,
				time.Since(reqStart).Milliseconds(),
				err,
			)
			if !tw.WroteHeader {
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
			}
			return
		}
		log.Printf("[proxy][done] token=%s action=%s method=%s range=%q probePreflight=%t status=%d wrote=%d durationMs=%d resolvedType=%q",
			token,
			action,
			r.Method,
			rangeHeader,
			probePreflight,
			tw.StatusCode,
			tw.BytesWritten,
			time.Since(reqStart).Milliseconds(),
			strings.TrimSpace(resolvedType),
		)
		if action == "" && strings.TrimSpace(currentContentType) == "" && strings.TrimSpace(resolvedType) != "" {
			s.setDetectedContentType(token, resolvedType)
		}
	})

	// Register endpoint: POST /register (or /<basePath>/register)
	mux.HandleFunc(mountPath(basePath, "/register"), func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		writeCORSHeaders(w)

		var in struct {
			URL         string          `json:"url"`
			Headers     json.RawMessage `json:"headers"`
			Header      json.RawMessage `json:"header"`
			HeadersList []headerLine    `json:"headersList"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		in.URL = strings.TrimSpace(in.URL)
		if in.URL == "" {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		u, err := url.Parse(in.URL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		hostLower := strings.ToLower(strings.TrimSpace(u.Hostname()))
		if hostLower == "0.0.0.0" || hostLower == "127.0.0.1" || hostLower == "localhost" || hostLower == "::1" {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}
		if ip := net.ParseIP(hostLower); ip != nil && (ip.IsLoopback() || ip.IsPrivate()) {
			http.Error(w, "Bad Request", http.StatusBadRequest)
			return
		}

		var hdrLines []headerLine
		if len(in.HeadersList) > 0 {
			hdrLines = sanitizeHeaderLines(in.HeadersList)
		} else if len(in.Headers) > 0 || len(in.Header) > 0 {
			// Backward-compatible:
			// - {"headers": {"k":"v"}} / {"headers":[{"key":"k","value":"v"}...]}
			// - {"header":  {"k":"v"}} / {"header":[{"key":"k","value":"v"}...]}
			raw := in.Headers
			if len(raw) == 0 {
				raw = in.Header
			}
			var m map[string]string
			if err := json.Unmarshal(raw, &m); err == nil {
				hdrLines = sanitizeHeaderLines(mapToHeaderLines(m))
			} else {
				var list []headerLine
				if err2 := json.Unmarshal(raw, &list); err2 == nil {
					hdrLines = sanitizeHeaderLines(list)
				}
			}
		}

		token := s.put(&entry{
			URL:                 in.URL,
			HeaderLines:         hdrLines,
			ContentType:         "",
			FallbackContentType: inferContentTypeFromURL(in.URL),
		})

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": token})
	})

	// Version endpoint: GET /version (or /<basePath>/version)
	mux.HandleFunc(mountPath(basePath, "/version"), func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			writeCORSHeaders(w)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		writeCORSHeaders(w)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string]string{"version": serverVersion()})
	})

	server := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}

	if basePath != "" {
		log.Printf("Go proxy base path: %s", basePath)
	}
	log.Printf("Go proxy listening on %s (ttl=%s)", listen, s.ttl)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-restart:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = server.Shutdown(ctx)
		cancel()
		<-errCh
		return errRestartRequested
	case err := <-errCh:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func main() {
	log.Printf("Go proxy version : %s", serverVersion())

	listen := fmt.Sprintf("%s:%d", defaultListenHost, defaultListenPort)
	tokenTTL := time.Duration(defaultTokenTTLSeconds) * time.Second

	cfgPath := findConfigPath()
	if err := ensureDefaultConfigFile(cfgPath); err != nil {
		log.Fatalf("failed to initialize config file %q: %v", cfgPath, err)
	}

	// Shared transport/client: reuse connections across range requests to improve throughput.
	transport := &http.Transport{
		// IMPORTANT: do not use env proxies (HTTP_PROXY/HTTPS_PROXY). In many home-server setups those
		// are set for browsers (e.g. 127.0.0.1:7890) and will silently throttle/break large streaming.
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		// Disable upstream HTTP/2 to match Node's behavior and avoid H2 flow-control/buffering quirks
		// observed on some storage/CDN endpoints during long ranged media reads.
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   128,
		IdleConnTimeout:       120 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
		ReadBufferSize:        64 * 1024,
		WriteBufferSize:       64 * 1024,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   0,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	s := newStore(tokenTTL)
	go func() {
		t := time.NewTicker(5 * time.Minute)
		defer t.Stop()
		for range t.C {
			s.prune()
		}
	}()

	for {
		cfg, err := loadConfig(cfgPath)
		if err != nil {
			log.Printf("[config] load failed: %v (using defaults)", err)
			cfg = defaultConfig()
			cfg.BasePath = normalizeBasePath(cfg.BasePath)
		}

		stopWatch := make(chan struct{})
		restart := make(chan struct{}, 1)
		go watchConfig(cfgPath, cfg, stopWatch, restart)

		err = serveOnce(client, s, listen, cfg, restart)
		close(stopWatch)

		if errors.Is(err, errRestartRequested) {
			continue
		}
		if err != nil {
			log.Fatal(err)
		}
		return
	}
}

func writeCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Range, If-Range, Content-Type, Authorization, Accept, Accept-Language, If-None-Match, If-Modified-Since")
	w.Header().Set("Access-Control-Expose-Headers", "Accept-Ranges, Content-Range, Content-Length")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS, POST")
}

type trackedWriter struct {
	http.ResponseWriter
	WroteHeader  bool
	StatusCode   int
	BytesWritten int64
}

func (tw *trackedWriter) WriteHeader(code int) {
	tw.WroteHeader = true
	tw.StatusCode = code
	tw.ResponseWriter.WriteHeader(code)
}

func (tw *trackedWriter) Write(p []byte) (int, error) {
	if !tw.WroteHeader {
		tw.WriteHeader(http.StatusOK)
	}
	n, err := tw.ResponseWriter.Write(p)
	tw.BytesWritten += int64(n)
	return n, err
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnCloseReadCloser) Close() error {
	err := c.ReadCloser.Close()
	if c.cancel != nil {
		c.cancel()
	}
	return err
}

func proxyStream(
	client *http.Client,
	w http.ResponseWriter,
	r *http.Request,
	target string,
	headers []headerLine,
	forcedContentType string,
	currentContentType string,
	fallbackContentType string,
	headOnly bool,
	opts streamOptions,
) (string, error) {
	opts = normalizeStreamOptions(opts)
	rangeHeader := strings.TrimSpace(r.Header.Get("Range"))
	ifRange := r.Header.Get("If-Range")

	// For very large ranges, some upstreams throttle heavily. Work around by stitching
	// smaller upstream range requests into a single client response.
	if !headOnly && rangeHeader != "" {
		if start, end, ok := parseRangeBytes(rangeHeader); ok && start >= 0 {
			effectiveHedgeConns := opts.ChunkHedgeConns
			// Keep hedge enabled for all chunked ranges:
			// first request starts immediately, extra hedged request(s) start after delay.
			if opts.BypassChunking && strings.EqualFold(rangeHeader, "bytes=0-") {
				probeType, probeTotal, probeStatus, perr := probeOpenRangeMetadata(
					r.Context(),
					client,
					target,
					headers,
					ifRange,
					opts.ProbeDirectConns,
				)
				if perr != nil {
					log.Printf("[proxy][probe-meta] range=%q conns=%d err=%v", rangeHeader, opts.ProbeDirectConns, perr)
				} else {
					log.Printf("[proxy][probe-meta] range=%q conns=%d status=%d total=%d contentType=%q",
						rangeHeader, opts.ProbeDirectConns, probeStatus, probeTotal, strings.TrimSpace(probeType))
					if strings.TrimSpace(currentContentType) == "" && strings.TrimSpace(probeType) != "" {
						currentContentType = strings.TrimSpace(probeType)
					}
				}
			}
			log.Printf("[proxy][mode] chunked range=%q chunk=%d pool=%d timeoutMs=%d retries=%d hedge=%d hedgeDelayMs=%d",
				rangeHeader, opts.ChunkSize, opts.PoolSize, opts.Timeout.Milliseconds(), opts.Retries, effectiveHedgeConns, opts.ChunkHedgeDelay.Milliseconds())
			return proxyStreamChunked(
				client,
				w,
				r,
				target,
				headers,
				forcedContentType,
				currentContentType,
				fallbackContentType,
				ifRange,
				start,
				end,
				opts.ChunkSize,
				opts.PoolSize,
				opts.Timeout,
				opts.Retries,
				effectiveHedgeConns,
				opts.ChunkHedgeDelay,
			)
		}
	}
	if rangeHeader != "" {
		log.Printf("[proxy][mode] direct range=%q", rangeHeader)
	}

	upRes, err := followRedirects(r.Context(), client, target, headers, rangeHeader, ifRange)
	if err != nil {
		return "", err
	}
	defer upRes.Body.Close()

	writeCORSHeaders(w)
	w.Header().Set("X-Accel-Buffering", "no")

	copyHeader(w.Header(), upRes.Header)
	resolvedContentType := strings.TrimSpace(forcedContentType)
	if resolvedContentType == "" {
		resolvedContentType = chooseContentType(upRes.Header.Get("Content-Type"), currentContentType)
		if strings.TrimSpace(resolvedContentType) == "" {
			resolvedContentType = chooseContentType(upRes.Header.Get("Content-Type"), fallbackContentType)
		}
	}
	if strings.TrimSpace(resolvedContentType) != "" {
		w.Header().Set("Content-Type", strings.TrimSpace(resolvedContentType))
	}
	w.WriteHeader(upRes.StatusCode)

	if headOnly {
		return resolvedContentType, nil
	}
	return resolvedContentType, streamCopy(w, upRes.Body)
}

func followRedirects(ctx context.Context, client *http.Client, target string, headers []headerLine, rangeHeader string, ifRange string) (*http.Response, error) {
	cur := target
	for i := 0; i < 10; i++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, cur, nil)
		if err != nil {
			return nil, err
		}

		hostOverride := applyHeaderLines(req, headers)
		if hostOverride != "" {
			req.Host = hostOverride
		}

		// Range/If-Range are controlled by proxy logic (chunking, etc).
		// When present, drop any registered Range/If-Range and apply the selected values.
		if rangeHeader != "" {
			req.Header.Del("Range")
			req.Header.Add("Range", rangeHeader)
		}
		if ifRange != "" {
			req.Header.Del("If-Range")
			req.Header.Add("If-Range", ifRange)
		}

		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if res.StatusCode >= 300 && res.StatusCode < 400 {
			loc := strings.TrimSpace(res.Header.Get("Location"))
			if loc == "" {
				return res, nil
			}
			res.Body.Close()
			next, err := resolveLocation(cur, loc)
			if err != nil {
				return nil, err
			}
			cur = next
			continue
		}
		return res, nil
	}
	return nil, fmt.Errorf("too many redirects")
}

func followRedirectsHedged(
	ctx context.Context,
	client *http.Client,
	target string,
	headers []headerLine,
	rangeHeader string,
	ifRange string,
	concurrency int,
) (*http.Response, error) {
	if concurrency <= 1 {
		return followRedirects(ctx, client, target, headers, rangeHeader, ifRange)
	}
	type result struct {
		index int
		res   *http.Response
		err   error
	}
	resCh := make(chan result, concurrency)
	cancels := make([]context.CancelFunc, concurrency)
	for i := 0; i < concurrency; i++ {
		reqCtx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel
		go func(idx int, cctx context.Context) {
			res, err := followRedirects(cctx, client, target, headers, rangeHeader, ifRange)
			resCh <- result{index: idx, res: res, err: err}
		}(i, reqCtx)
	}

	var winner *http.Response
	var winnerIdx = -1
	var lastErr error
	for i := 0; i < concurrency; i++ {
		r := <-resCh
		if r.err == nil && r.res != nil && winner == nil {
			winner = r.res
			winnerIdx = r.index
			for j, cancel := range cancels {
				if j != winnerIdx && cancel != nil {
					cancel()
				}
			}
			continue
		}
		if r.err != nil {
			lastErr = r.err
		}
		if r.res != nil {
			_ = r.res.Body.Close()
		}
	}
	if winner != nil {
		return winner, nil
	}
	if lastErr == nil {
		lastErr = context.Canceled
	}
	return nil, lastErr
}

func probeOpenRangeMetadata(
	ctx context.Context,
	client *http.Client,
	target string,
	headers []headerLine,
	ifRange string,
	concurrency int,
) (contentType string, total int64, status int, err error) {
	if concurrency <= 1 {
		concurrency = 1
	}
	// Probe metadata with a minimal ranged request; this is only for quickly resolving
	// size/type before we switch back to chunked acceleration for actual bytes.
	res, err := followRedirectsHedged(ctx, client, target, headers, "bytes=0-0", ifRange, concurrency)
	if err != nil {
		return "", 0, 0, err
	}
	defer res.Body.Close()

	_, _ = io.CopyN(io.Discard, res.Body, 1)
	status = res.StatusCode
	contentType = strings.TrimSpace(res.Header.Get("Content-Type"))
	if res.StatusCode == http.StatusPartialContent {
		if t, ok := parseContentRangeTotal(res.Header.Get("Content-Range")); ok {
			total = t
		}
	} else if res.StatusCode == http.StatusOK {
		if n, perr := strconv.ParseInt(strings.TrimSpace(res.Header.Get("Content-Length")), 10, 64); perr == nil && n > 0 {
			total = n
		}
	}
	return contentType, total, status, nil
}

func fetchWithRetry(
	ctx context.Context,
	client *http.Client,
	target string,
	headers []headerLine,
	rangeHeader string,
	ifRange string,
	timeout time.Duration,
	retries int,
) (*http.Response, error) {
	attempts := retries + 1
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reqCtx := ctx
		cancel := func() {}
		if timeout > 0 {
			reqCtx, cancel = context.WithTimeout(ctx, timeout)
		}
		res, err := followRedirects(reqCtx, client, target, headers, rangeHeader, ifRange)
		if err == nil {
			if timeout > 0 && res != nil && res.Body != nil {
				res.Body = &cancelOnCloseReadCloser{ReadCloser: res.Body, cancel: cancel}
			} else {
				cancel()
			}
			return res, nil
		}
		cancel()
		// Hedged losers are canceled intentionally; do not retry/cascade.
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		lastErr = err
		log.Printf("[proxy][upstream-retry] range=%q attempt=%d/%d timeoutMs=%d errType=%T err=%s",
			rangeHeader,
			i+1,
			attempts,
			timeout.Milliseconds(),
			err,
			trimErrMsg(err),
		)
		if i+1 >= attempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if lastErr == nil {
		lastErr = context.DeadlineExceeded
	}
	return nil, lastErr
}

func resolveLocation(baseStr string, loc string) (string, error) {
	baseURL, err := url.Parse(baseStr)
	if err != nil {
		return "", err
	}
	locURL, err := url.Parse(loc)
	if err != nil {
		return "", err
	}
	return baseURL.ResolveReference(locURL).String(), nil
}

func copyHeader(dst http.Header, src http.Header) {
	hopByHop := map[string]bool{
		"connection":          true,
		"keep-alive":          true,
		"proxy-authenticate":  true,
		"proxy-authorization": true,
		"te":                  true,
		"trailer":             true,
		"transfer-encoding":   true,
		"upgrade":             true,
	}
	for k, vv := range src {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "access-control-") {
			continue
		}
		if hopByHop[lk] {
			continue
		}
		dst.Del(k)
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func streamCopy(w http.ResponseWriter, r io.Reader) error {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 256*1024)
	lastFlush := time.Now()
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
			if flusher != nil && time.Since(lastFlush) > 250*time.Millisecond {
				flusher.Flush()
				lastFlush = time.Now()
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	if flusher != nil {
		flusher.Flush()
	}
	return nil
}

func trimErrMsg(err error) string {
	if err == nil {
		return ""
	}
	s := strings.TrimSpace(err.Error())
	if s == "" {
		return ""
	}
	// Avoid leaking full upstream URLs in logs.
	if i := strings.Index(s, `": `); i >= 0 && strings.Contains(s[:i], "http") {
		return s[i+3:]
	}
	if i := strings.Index(s, "http://"); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	if i := strings.Index(s, "https://"); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

func parseRangeBytes(header string) (start int64, end int64, ok bool) {
	h := strings.TrimSpace(header)
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	// Only handle single range.
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	if parts[0] == "" {
		// Suffix range (bytes=-N) not supported in chunking.
		return 0, 0, false
	}
	s, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || s < 0 {
		return 0, 0, false
	}
	start = s
	end = -1
	if strings.TrimSpace(parts[1]) != "" {
		e, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || e < start {
			return 0, 0, false
		}
		end = e
	}
	return start, end, true
}

func parseContentRangeTotal(h string) (total int64, ok bool) {
	// e.g. "bytes 0-1/919354528"
	s := strings.TrimSpace(h)
	if !strings.HasPrefix(s, "bytes") {
		return 0, false
	}
	s = strings.TrimSpace(strings.TrimPrefix(s, "bytes"))
	slash := strings.LastIndex(s, "/")
	if slash < 0 || slash == len(s)-1 {
		return 0, false
	}
	totalStr := strings.TrimSpace(s[slash+1:])
	if totalStr == "*" {
		return 0, false
	}
	t, err := strconv.ParseInt(totalStr, 10, 64)
	if err != nil || t <= 0 {
		return 0, false
	}
	return t, true
}

func proxyStreamChunked(
	client *http.Client,
	w http.ResponseWriter,
	r *http.Request,
	target string,
	headers []headerLine,
	forcedContentType string,
	currentContentType string,
	fallbackContentType string,
	ifRange string,
	start int64,
	end int64,
	chunkSize int64,
	poolSize int,
	timeout time.Duration,
	retries int,
	chunkHedgeConns int,
	chunkHedgeDelay time.Duration,
) (string, error) {
	ctx := r.Context()
	curStart := start
	total := int64(0)
	firstChunkReady := false
	headerSource := http.Header(nil)

	// First request: get headers + total size (and possibly compute open-ended end).
	firstEnd := end
	if firstEnd < 0 {
		firstEnd = start + chunkSize - 1
	} else if firstEnd > start+chunkSize-1 {
		firstEnd = start + chunkSize - 1
	}
	firstRange := fmt.Sprintf("bytes=%d-%d", curStart, firstEnd)
	upRes, err := fetchWithRetry(ctx, client, target, headers, firstRange, ifRange, timeout, retries)
	if err != nil {
		return "", err
	}
	defer upRes.Body.Close()

	resolvedContentType := strings.TrimSpace(forcedContentType)
	if resolvedContentType == "" {
		resolvedContentType = chooseContentType(upRes.Header.Get("Content-Type"), currentContentType)
		if strings.TrimSpace(resolvedContentType) == "" {
			resolvedContentType = chooseContentType(upRes.Header.Get("Content-Type"), fallbackContentType)
		}
	}

	if upRes.StatusCode == http.StatusPartialContent {
		headerSource = upRes.Header
		firstChunkReady = true
		var ok bool
		total, ok = parseContentRangeTotal(upRes.Header.Get("Content-Range"))
		if !ok {
			return "", fmt.Errorf("upstream missing/invalid Content-Range")
		}
	} else {
		// Some upstream links may answer 200 for the first probe range. We still enforce
		// chunked range mode by probing range capability with bytes=0-0, then fetching
		// each requested chunk explicitly.
		log.Printf("[proxy][chunk-probe] firstRange=%q firstStatus=%d; probing bytes=0-0", firstRange, upRes.StatusCode)
		_ = upRes.Body.Close()
		probeRes, perr := fetchWithRetry(ctx, client, target, headers, "bytes=0-0", ifRange, timeout, retries)
		if perr != nil {
			return "", perr
		}
		defer probeRes.Body.Close()
		if probeRes.StatusCode != http.StatusPartialContent {
			return "", fmt.Errorf("upstream range probe failed status=%d", probeRes.StatusCode)
		}
		headerSource = probeRes.Header
		var ok bool
		total, ok = parseContentRangeTotal(probeRes.Header.Get("Content-Range"))
		if !ok {
			return "", fmt.Errorf("upstream range probe missing/invalid Content-Range")
		}
		if strings.TrimSpace(resolvedContentType) == "" {
			resolvedContentType = chooseContentType(probeRes.Header.Get("Content-Type"), currentContentType)
			if strings.TrimSpace(resolvedContentType) == "" {
				resolvedContentType = chooseContentType(probeRes.Header.Get("Content-Type"), fallbackContentType)
			}
		}
	}
	if end < 0 {
		end = total - 1
	}
	if end >= total {
		end = total - 1
	}
	if end < start {
		return "", fmt.Errorf("invalid resolved range")
	}

	// Write client headers for the full requested range.
	writeCORSHeaders(w)
	w.Header().Set("X-Accel-Buffering", "no")
	copyHeader(w.Header(), headerSource)
	if strings.TrimSpace(resolvedContentType) != "" {
		w.Header().Set("Content-Type", strings.TrimSpace(resolvedContentType))
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", (end-start)+1))
	w.WriteHeader(http.StatusPartialContent)

	if firstChunkReady {
		// Stream the first chunk from the already-open upstream response.
		if err := streamCopy(w, upRes.Body); err != nil {
			return "", err
		}
		curStart = firstEnd + 1
	}
	if curStart > end {
		return resolvedContentType, nil
	}

	windowBytes := int64(defaultChunkWindowBytes)
	if windowBytes < chunkSize {
		windowBytes = chunkSize
	}
	firstBlockFirstChunkHedge := true
	for blockStart := curStart; blockStart <= end; {
		blockEnd := blockStart + windowBytes - 1
		if blockEnd > end {
			blockEnd = end
		}
		if err := streamChunkBlock(ctx, client, w, target, headers, ifRange, blockStart, blockEnd, chunkSize, poolSize, timeout, retries, chunkHedgeConns, chunkHedgeDelay, firstBlockFirstChunkHedge); err != nil {
			return "", err
		}
		firstBlockFirstChunkHedge = false
		blockStart = blockEnd + 1
	}
	return resolvedContentType, nil
}

func streamChunkBlock(
	ctx context.Context,
	client *http.Client,
	w http.ResponseWriter,
	target string,
	headers []headerLine,
	ifRange string,
	blockStart int64,
	blockEnd int64,
	chunkSize int64,
	poolSize int,
	timeout time.Duration,
	retries int,
	chunkHedgeConns int,
	chunkHedgeDelay time.Duration,
	firstChunkHedge bool,
) error {
	type chunkJob struct {
		Index int
		Start int64
		End   int64
	}
	type chunkResult struct {
		Index int
		Body  []byte
		Err   error
	}
	jobs := make([]chunkJob, 0, 128)
	for i, cur := 0, blockStart; cur <= blockEnd; i++ {
		curEnd := cur + chunkSize - 1
		if curEnd > blockEnd {
			curEnd = blockEnd
		}
		jobs = append(jobs, chunkJob{Index: i, Start: cur, End: curEnd})
		cur = curEnd + 1
	}
	if len(jobs) == 0 {
		return nil
	}
	if poolSize <= 1 {
		for idx, j := range jobs {
			rh := fmt.Sprintf("bytes=%d-%d", j.Start, j.End)
			hedgeConns := 1
			if firstChunkHedge && idx == 0 {
				hedgeConns = chunkHedgeConns
			}
			body, err := fetchChunkBodyWithHedge(ctx, client, target, headers, rh, ifRange, timeout, retries, hedgeConns, chunkHedgeDelay)
			if err != nil {
				return err
			}
			if len(body) > 0 {
				if _, werr := w.Write(body); werr != nil {
					return werr
				}
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
		return nil
	}
	if poolSize > len(jobs) {
		poolSize = len(jobs)
	}

	// Ordered batched dispatch:
	// Request only a contiguous batch (size<=poolSize) at a time, then write in-order.
	// This avoids issuing too many tail chunks before head chunks are ready.
	flusher, _ := w.(http.Flusher)
	for base := 0; base < len(jobs); base += poolSize {
		limit := base + poolSize
		if limit > len(jobs) {
			limit = len(jobs)
		}
		batch := jobs[base:limit]
		batchRes := make([][]byte, len(batch))
		errCh := make(chan chunkResult, len(batch))

		for i, j := range batch {
			i := i
			j := j
			go func() {
				rh := fmt.Sprintf("bytes=%d-%d", j.Start, j.End)
				hedgeConns := 1
				if firstChunkHedge && base == 0 && i == 0 {
					hedgeConns = chunkHedgeConns
				}
				body, err := fetchChunkBodyWithHedge(ctx, client, target, headers, rh, ifRange, timeout, retries, hedgeConns, chunkHedgeDelay)
				if err != nil {
					errCh <- chunkResult{Index: i, Err: err}
					return
				}
				errCh <- chunkResult{Index: i, Body: body}
			}()
		}

		var firstErr error
		for i := 0; i < len(batch); i++ {
			res := <-errCh
			if res.Err != nil && firstErr == nil {
				firstErr = res.Err
				continue
			}
			if res.Err == nil {
				batchRes[res.Index] = res.Body
			}
		}
		if firstErr != nil {
			return firstErr
		}

		for _, body := range batchRes {
			if len(body) == 0 {
				continue
			}
			if _, err := w.Write(body); err != nil {
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
	return nil
}

func fetchChunkBodyWithHedge(
	ctx context.Context,
	client *http.Client,
	target string,
	headers []headerLine,
	rangeHeader string,
	ifRange string,
	timeout time.Duration,
	retries int,
	hedgeConns int,
	hedgeDelay time.Duration,
) ([]byte, error) {
	if hedgeConns <= 1 {
		res, err := fetchWithRetry(ctx, client, target, headers, rangeHeader, ifRange, timeout, retries)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusPartialContent {
			return nil, fmt.Errorf("upstream status=%d for %s", res.StatusCode, rangeHeader)
		}
		return io.ReadAll(res.Body)
	}
	log.Printf("[proxy][chunk-hedge] range=%q conns=%d delayMs=%d", rangeHeader, hedgeConns, hedgeDelay.Milliseconds())

	type chunkFetchResult struct {
		body []byte
		err  error
	}
	hedgeCtx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	resCh := make(chan chunkFetchResult, hedgeConns)

	for i := 0; i < hedgeConns; i++ {
		go func(idx int) {
			if idx > 0 && hedgeDelay > 0 {
				t := time.NewTimer(time.Duration(idx) * hedgeDelay)
				defer t.Stop()
				select {
				case <-hedgeCtx.Done():
					resCh <- chunkFetchResult{err: hedgeCtx.Err()}
					return
				case <-t.C:
				}
			}
			res, err := fetchWithRetry(hedgeCtx, client, target, headers, rangeHeader, ifRange, timeout, retries)
			if err != nil {
				resCh <- chunkFetchResult{err: err}
				return
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusPartialContent {
				resCh <- chunkFetchResult{err: fmt.Errorf("upstream status=%d for %s", res.StatusCode, rangeHeader)}
				return
			}
			body, readErr := io.ReadAll(res.Body)
			if readErr != nil {
				resCh <- chunkFetchResult{err: readErr}
				return
			}
			resCh <- chunkFetchResult{body: body}
		}(i)
	}

	var firstErr error
	for i := 0; i < hedgeConns; i++ {
		r := <-resCh
		if r.err == nil {
			cancelAll()
			return r.body, nil
		}
		if !errors.Is(r.err, context.Canceled) && !errors.Is(r.err, context.DeadlineExceeded) {
			firstErr = r.err
		} else if firstErr == nil {
			firstErr = r.err
		}
	}
	if firstErr == nil {
		firstErr = context.DeadlineExceeded
	}
	return nil, firstErr
}

func mountPath(basePath, suffix string) string {
	if basePath == "" {
		return suffix
	}
	if suffix == "" || suffix == "/" {
		return basePath
	}
	if strings.HasPrefix(suffix, "/") {
		return basePath + suffix
	}
	return basePath + "/" + suffix
}
