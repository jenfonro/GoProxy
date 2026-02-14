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
	"net"
	"net/http"
	"net/url"
	"os"
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
	defaultUpstreamChunkSize = 32 * 1024 * 1024

	defaultConfigPollInterval = 1 * time.Second
	defaultConfigDebounce     = 200 * time.Millisecond
)

type entry struct {
	URL         string       `json:"url"`
	HeaderLines []headerLine `json:"headers"`
	TS          time.Time    `json:"-"`
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
		if err := proxyStream(client, tw, r, up.String(), nil, r.Method == http.MethodHead); err != nil {
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

		tw := &trackedWriter{ResponseWriter: w}
		if err := proxyStream(client, tw, r, target, e.HeaderLines, r.Method == http.MethodHead); err != nil {
			log.Printf("[proxy] token=%s error=%v", token, err)
			if !tw.WroteHeader {
				http.Error(w, "Bad Gateway", http.StatusBadGateway)
			}
			return
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
		} else if len(in.Headers) > 0 {
			// Backward-compatible: accept {"headers": {"k":"v"}} or {"headers":[{"key":"k","value":"v"}...]}
			var m map[string]string
			if err := json.Unmarshal(in.Headers, &m); err == nil {
				hdrLines = sanitizeHeaderLines(mapToHeaderLines(m))
			} else {
				var list []headerLine
				if err2 := json.Unmarshal(in.Headers, &list); err2 == nil {
					hdrLines = sanitizeHeaderLines(list)
				}
			}
		}

		token := s.put(&entry{URL: in.URL, HeaderLines: hdrLines})

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
	WroteHeader bool
}

func (tw *trackedWriter) WriteHeader(code int) {
	tw.WroteHeader = true
	tw.ResponseWriter.WriteHeader(code)
}

func proxyStream(client *http.Client, w http.ResponseWriter, r *http.Request, target string, headers []headerLine, headOnly bool) error {
	rangeHeader := r.Header.Get("Range")
	ifRange := r.Header.Get("If-Range")

	// For very large ranges, some upstreams throttle heavily. Work around by stitching
	// multiple smaller upstream range requests into a single client response.
	if !headOnly && rangeHeader != "" {
		if start, end, ok := parseRangeBytes(rangeHeader); ok && start >= 0 {
			// Only chunk when the requested length is large OR open-ended.
			threshold := int64(defaultChunkThresholdB)
			chunkSize := int64(defaultUpstreamChunkSize)
			if chunkSize < 1024*1024 {
				chunkSize = 1024 * 1024
			}
			openEnded := end < 0
			if openEnded || (end-start+1) > threshold {
				return proxyStreamChunked(client, w, r, target, headers, ifRange, start, end, chunkSize)
			}
		}
	}

	upRes, err := followRedirects(r.Context(), client, target, headers, rangeHeader, ifRange)
	if err != nil {
		return err
	}
	defer upRes.Body.Close()

	writeCORSHeaders(w)
	w.Header().Set("X-Accel-Buffering", "no")

	copyHeader(w.Header(), upRes.Header)
	w.WriteHeader(upRes.StatusCode)

	if headOnly {
		return nil
	}
	return streamCopy(w, upRes.Body)
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

func proxyStreamChunked(client *http.Client, w http.ResponseWriter, r *http.Request, target string, headers []headerLine, ifRange string, start int64, end int64, chunkSize int64) error {
	ctx := r.Context()
	curStart := start

	// First request: get headers + total size (and possibly compute open-ended end).
	firstEnd := end
	if firstEnd < 0 {
		firstEnd = start + chunkSize - 1
	} else if firstEnd > start+chunkSize-1 {
		firstEnd = start + chunkSize - 1
	}
	firstRange := fmt.Sprintf("bytes=%d-%d", curStart, firstEnd)
	upRes, err := followRedirects(ctx, client, target, headers, firstRange, ifRange)
	if err != nil {
		return err
	}
	defer upRes.Body.Close()

	// If upstream doesn't honor ranges, just pass through.
	if upRes.StatusCode != http.StatusPartialContent {
		writeCORSHeaders(w)
		w.Header().Set("X-Accel-Buffering", "no")
		copyHeader(w.Header(), upRes.Header)
		w.WriteHeader(upRes.StatusCode)
		return streamCopy(w, upRes.Body)
	}

	total, ok := parseContentRangeTotal(upRes.Header.Get("Content-Range"))
	if !ok {
		return fmt.Errorf("upstream missing/invalid Content-Range")
	}
	if end < 0 {
		end = total - 1
	}
	if end >= total {
		end = total - 1
	}
	if end < start {
		return fmt.Errorf("invalid resolved range")
	}

	// Write client headers for the full requested range.
	writeCORSHeaders(w)
	w.Header().Set("X-Accel-Buffering", "no")
	copyHeader(w.Header(), upRes.Header)
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", (end-start)+1))
	w.WriteHeader(http.StatusPartialContent)

	// Stream the first chunk.
	if err := streamCopy(w, upRes.Body); err != nil {
		return err
	}

	curStart = firstEnd + 1
	for curStart <= end {
		curEnd := curStart + chunkSize - 1
		if curEnd > end {
			curEnd = end
		}
		rh := fmt.Sprintf("bytes=%d-%d", curStart, curEnd)
		res, err := followRedirects(ctx, client, target, headers, rh, ifRange)
		if err != nil {
			return err
		}
		// Always close before next iteration.
		func() {
			defer res.Body.Close()
			if res.StatusCode != http.StatusPartialContent {
				err = fmt.Errorf("upstream status=%d for %s", res.StatusCode, rh)
				return
			}
			err = streamCopy(w, res.Body)
		}()
		if err != nil {
			return err
		}
		curStart = curEnd + 1
	}
	return nil
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
