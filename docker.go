package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// docker is a minimal Docker Engine API client: list, inspect, start and stop are all woc
// needs, so there is no SDK. Reads and writes can go to different endpoints: in production two
// docker-socket-proxies, one read-only and one that only accepts container start and stop
// (a single proxy can't express that: allowing POST on /containers also allows create, delete
// and exec).
type docker struct {
	r, w *endpoint
}

type endpoint struct {
	base string // http://host:port
	hc   *http.Client
}

func newDocker(readHost, writeHost string) (*docker, error) {
	r, err := newEndpoint(readHost)
	if err != nil {
		return nil, err
	}
	w := r
	if writeHost != "" && writeHost != readHost {
		if w, err = newEndpoint(writeHost); err != nil {
			return nil, err
		}
	}
	return &docker{r: r, w: w}, nil
}

func newEndpoint(host string) (*endpoint, error) {
	u, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("DOCKER_HOST %q: %w", host, err)
	}
	switch u.Scheme {
	case "tcp", "http":
		return &endpoint{base: "http://" + u.Host, hc: &http.Client{Timeout: 30 * time.Second}}, nil
	case "unix":
		tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", u.Path)
		}}
		return &endpoint{base: "http://docker", hc: &http.Client{Timeout: 30 * time.Second, Transport: tr}}, nil
	}
	return nil, fmt.Errorf("DOCKER_HOST %q: use tcp://, http:// or unix://", host)
}

type containerSummary struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Labels map[string]string `json:"Labels"`
	State  string            `json:"State"`
}

// Name without the leading slash the API puts in front.
func (c containerSummary) Name() string {
	if len(c.Names) == 0 {
		return c.ID
	}
	return strings.TrimPrefix(c.Names[0], "/")
}

type containerState struct {
	Running bool `json:"Running"`
	Health  *struct {
		Status string `json:"Status"`
	} `json:"Health"`
}

// ready reports whether the container runs and, when it declares a healthcheck, passes it.
func (s containerState) ready() bool {
	return s.Running && (s.Health == nil || s.Health.Status == "healthy")
}

func (d *endpoint) do(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, d.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := d.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 304 = already started / already stopped: the state we wanted.
	if resp.StatusCode == http.StatusNotModified {
		return nil
	}
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("docker %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// listManaged returns every container, running or not, that opted in with woc.enable=true.
func (d *docker) listManaged(ctx context.Context) ([]containerSummary, error) {
	f, _ := json.Marshal(map[string][]string{"label": {labelEnable + "=true"}})
	var out []containerSummary
	err := d.r.do(ctx, http.MethodGet, "/containers/json?all=1&filters="+url.QueryEscape(string(f)), &out)
	return out, err
}

func (d *docker) inspect(ctx context.Context, name string) (containerState, error) {
	var out struct {
		State containerState `json:"State"`
	}
	err := d.r.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(name)+"/json", &out)
	return out.State, err
}

func (d *docker) start(ctx context.Context, name string) error {
	return d.w.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(name)+"/start", nil)
}

func (d *docker) stop(ctx context.Context, name string, grace time.Duration) error {
	return d.w.do(ctx, http.MethodPost, fmt.Sprintf("/containers/%s/stop?t=%d", url.PathEscape(name), int(grace.Seconds())), nil)
}
