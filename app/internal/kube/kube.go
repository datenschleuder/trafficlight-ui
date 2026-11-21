// Package kube ist ein minimaler Kubernetes-Client (Pods auflisten, Logs streamen)
// auf Basis der Standardbibliothek.
package kube

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

type Config struct {
	URL       string // leer = In-Cluster (KUBERNETES_SERVICE_HOST/PORT)
	TokenFile string // leer = ServiceAccount-Token (bei In-Cluster)
	CAFile    string
}

type Client struct {
	base      string
	tokenFile string
	hc        *http.Client
}

type Container struct {
	Name         string
	RestartCount int
	Running      bool
	StartedAt    time.Time
	JSONHint     string // "json", "text" oder "" (aus args/env, nur ein Hinweis)
}

type Pod struct {
	Namespace, Name, UID, Node, Phase string
	Containers                        []Container
}

func New(cfg Config) (*Client, error) {
	c := &Client{base: strings.TrimRight(cfg.URL, "/"), tokenFile: cfg.TokenFile}
	caFile := cfg.CAFile
	if c.base == "" {
		host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
		if host == "" || port == "" {
			return nil, errors.New("weder KUBE_API_URL gesetzt noch im Cluster (KUBERNETES_SERVICE_HOST fehlt)")
		}
		c.base = "https://" + net.JoinHostPort(host, port)
		if c.tokenFile == "" {
			c.tokenFile = saDir + "/token"
		}
		if caFile == "" {
			caFile = saDir + "/ca.crt"
		}
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   4,
		ForceAttemptHTTP2:     true,
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("CA lesen: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s enthält kein gültiges Zertifikat", caFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	c.hc = &http.Client{Transport: tr}
	return c, nil
}

func (c *Client) do(ctx context.Context, path string, q url.Values) (*http.Response, error) {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	if c.tokenFile != "" {
		// Bei jedem Request neu lesen: projizierte Tokens werden vom Kubelet rotiert.
		tok, err := os.ReadFile(c.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("Token lesen: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	req.Header.Set("Accept", "application/json, */*")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		msg := strings.TrimSpace(string(body))
		var st struct{ Message string }
		if json.Unmarshal(body, &st) == nil && st.Message != "" {
			msg = st.Message
		}
		return nil, &StatusError{Code: resp.StatusCode, Msg: msg}
	}
	return resp, nil
}

type StatusError struct {
	Code int
	Msg  string
}

func (e *StatusError) Error() string { return fmt.Sprintf("kube-api %d: %s", e.Code, e.Msg) }

type podList struct {
	Items []struct {
		Metadata struct{ Name, Namespace, UID string }
		Spec     struct {
			NodeName   string
			Containers []struct {
				Name    string
				Args    []string
				Command []string
				Env     []struct{ Name, Value string }
			}
		}
		Status struct {
			Phase             string
			ContainerStatuses []struct {
				Name         string
				RestartCount int
				State        struct {
					Running *struct{ StartedAt time.Time }
				}
			}
		}
	}
}

// ListPods liefert alle Pods eines Namespaces, die zum Label-Selector passen.
func (c *Client) ListPods(ctx context.Context, namespace, selector string) ([]Pod, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := c.do(ctx, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/pods", url.Values{"labelSelector": {selector}})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var pl podList
	if err := json.NewDecoder(resp.Body).Decode(&pl); err != nil {
		return nil, fmt.Errorf("Pod-Liste lesen: %w", err)
	}
	pods := make([]Pod, 0, len(pl.Items))
	for _, it := range pl.Items {
		p := Pod{Namespace: it.Metadata.Namespace, Name: it.Metadata.Name, UID: it.Metadata.UID,
			Node: it.Spec.NodeName, Phase: it.Status.Phase}
		for _, sc := range it.Spec.Containers {
			ct := Container{Name: sc.Name}
			for _, arg := range append(sc.Command, sc.Args...) {
				if v, ok := strings.CutPrefix(strings.ToLower(arg), "--accesslog.format="); ok {
					ct.JSONHint = hint(v)
				}
			}
			for _, e := range sc.Env {
				if e.Name == "TRAEFIK_ACCESSLOG_FORMAT" {
					ct.JSONHint = hint(strings.ToLower(e.Value))
				}
			}
			for _, cs := range it.Status.ContainerStatuses {
				if cs.Name == sc.Name {
					ct.RestartCount = cs.RestartCount
					if cs.State.Running != nil {
						ct.Running, ct.StartedAt = true, cs.State.Running.StartedAt
					}
				}
			}
			p.Containers = append(p.Containers, ct)
		}
		pods = append(pods, p)
	}
	return pods, nil
}

func hint(v string) string {
	if strings.Trim(v, `"'`) == "json" {
		return "json"
	}
	return "text"
}

type LogOptions struct {
	Since    time.Time // nur Zeilen ab diesem Zeitpunkt (Sekundengenauigkeit)
	Previous bool      // Logs der vorherigen Container-Instanz
	Follow   bool
}

// Logs öffnet den Logstream eines Containers; jede Zeile beginnt mit dem
// Kubelet-Zeitstempel (RFC3339Nano) gefolgt von einem Leerzeichen.
func (c *Client) Logs(ctx context.Context, namespace, pod, container string, o LogOptions) (io.ReadCloser, error) {
	q := url.Values{"container": {container}, "timestamps": {"true"}}
	if o.Follow {
		q.Set("follow", "true")
	}
	if o.Previous {
		q.Set("previous", "true")
	}
	if !o.Since.IsZero() {
		q.Set("sinceTime", o.Since.UTC().Truncate(time.Second).Format(time.RFC3339))
	}
	resp, err := c.do(ctx, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/pods/"+url.PathEscape(pod)+"/log", q)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
