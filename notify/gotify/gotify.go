// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gotify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"

	commoncfg "github.com/prometheus/common/config"

	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
)

type Notifier struct {
	conf    *GotifyConfig
	tmpl    *template.Template
	logger  *slog.Logger
	client  *http.Client
	retrier *notify.Retrier
}

func New(c *GotifyConfig, t *template.Template, l *slog.Logger, httpOpts ...commoncfg.HTTPClientOption) (*Notifier, error) {
	client, err := notify.NewClientWithTracing(*c.HTTPConfig, "gotify", httpOpts...)
	if err != nil {
		return nil, err
	}

	return &Notifier{
		conf:    c,
		tmpl:    t,
		logger:  l,
		client:  client,
		retrier: &notify.Retrier{},
	}, nil
}

type messageExtrasClientDisplay struct {
	ContentType string `json:"contentType,omitempty"`
}

type messageExtras struct {
	ClientDisplay *messageExtrasClientDisplay `json:"client::display,omitempty"`
}

type messageRequest struct {
	Title    string         `json:"title,omitempty"`
	Message  string         `json:"message"`
	Priority int            `json:"priority,omitempty"`
	Extras   *messageExtras `json:"extras,omitempty"`
}

func (n *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	key, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		return false, err
	}
	logger := n.logger.With("group_key", key)
	logger.Debug("extracted group key")

	data := notify.GetTemplateData(ctx, n.tmpl, as, logger)
	var tmplErr error
	tmplText := notify.TmplText(n.tmpl, data, &tmplErr)

	var token string
	if n.conf.Token != "" {
		token = string(n.conf.Token)
	} else {
		b, err := os.ReadFile(n.conf.TokenFile)
		if err != nil {
			return false, fmt.Errorf("read token_file: %w", err)
		}
		token = strings.TrimSpace(string(b))
	}

	var url string
	if n.conf.URL != nil {
		url = n.conf.URL.String()
	} else {
		b, err := os.ReadFile(n.conf.URLFile)
		if err != nil {
			return false, fmt.Errorf("read url_file: %w", err)
		}
		url = strings.TrimSpace(string(b))
	}

	priority, err := strconv.Atoi(strings.TrimSpace(tmplText(n.conf.Priority)))
	if err != nil {
		return false, fmt.Errorf("parse priority: %w", err)
	}

	req := messageRequest{
		Title:    strings.TrimSpace(tmplText(n.conf.Title)),
		Message:  strings.TrimSpace(tmplText(n.conf.Message)),
		Priority: priority,
	}
	if req.Message == "" {
		req.Message = "(no details)"
	}
	if n.conf.ContentType != "" && n.conf.ContentType != "text/plain" {
		req.Extras = &messageExtras{ClientDisplay: &messageExtrasClientDisplay{ContentType: n.conf.ContentType}}
	}

	if tmplErr != nil {
		return false, tmplErr
	}

	if n.conf.Timeout > 0 {
		postCtx, cancel := context.WithTimeoutCause(ctx, n.conf.Timeout, fmt.Errorf("configured gotify timeout reached (%s)", n.conf.Timeout))
		defer cancel()
		ctx = postCtx
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(&req); err != nil {
		return false, err
	}

	hReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return false, err
	}
	hReq.Header.Set("Content-Type", "application/json")
	hReq.Header.Set("X-Gotify-Key", token)

	resp, err := n.client.Do(hReq)
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%w: %w", err, context.Cause(ctx))
		}
		return true, notify.RedactURL(err)
	}
	defer notify.Drain(resp)

	shouldRetry, err := n.retrier.Check(resp.StatusCode, resp.Body)
	if err != nil {
		return shouldRetry, notify.NewErrorWithReason(notify.GetFailureReasonFromStatusCode(resp.StatusCode), err)
	}
	return shouldRetry, nil
}
