package heartime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// pollInterval bounds how long a waiting delivery goes without re-evaluation,
// for example while the overlap policy defers it until another process reports.
const pollInterval = 5 * time.Second

// maxRetryDelay caps the delay between delivery attempts of one occurrence.
const maxRetryDelay int64 = 300

// RetryDelay is the wait in seconds after a delivery attempt before the next
// attempt of the same occurrence: 5 s doubling to at most 300 s.
func RetryDelay(attempts int) int64 {
	if attempts < 1 {
		return 0
	}
	delay := int64(5)
	for step := 1; step < attempts && delay < maxRetryDelay; step++ {
		delay *= 2
	}
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}

// Run evaluates and delivers due occurrences to one receiver until ctx is
// cancelled. Waking is derived from persisted deadlines, never from a global
// tick. The receiver must deduplicate by the Idempotency-Key occurrence id. A
// 2xx response records acknowledgement only; effects are known only through
// Report. The endpoint and credential are operator configuration.
func (s *Store) Run(ctx context.Context, endpoint, token string) error {
	target, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	loopback := target.Hostname() == "127.0.0.1" || target.Hostname() == "localhost" || target.Hostname() == "::1"
	if target.Scheme != "https" && !(target.Scheme == "http" && loopback) {
		return errors.New("relay endpoint must use HTTPS or loopback HTTP")
	}
	if token == "" {
		return errors.New("relay requires the receiver credential configured by the operator")
	}
	client := &http.Client{
		Timeout:       10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirects are refused") },
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := time.Now().UTC().Unix()
		if err := s.Evaluate(now); err != nil {
			return err
		}
		pending, err := s.Pending()
		if err != nil {
			return err
		}
		wake := int64(0)
		earlier := func(at int64) {
			if at > 0 && (wake == 0 || at < wake) {
				wake = at
			}
		}
		if len(pending) > 0 {
			earlier(now + int64(pollInterval/time.Second))
		}
		for _, delivery := range pending {
			if retryAt := delivery.LastAttempt + RetryDelay(delivery.Attempts); delivery.Attempts > 0 && retryAt > now {
				earlier(retryAt)
				continue
			}
			err := s.Attempt(delivery.ID, now)
			switch {
			case errors.Is(err, ErrOverlapDeferred), errors.Is(err, ErrNewerPending), errors.Is(err, ErrOutsideCoverage), errors.Is(err, ErrNotPending):
				continue
			case err != nil:
				return err
			}
			if deliver(ctx, client, endpoint, token, delivery) {
				if err := s.Acknowledge(delivery.ID, time.Now().UTC().Unix()); err != nil {
					return err
				}
			}
		}
		next, err := s.NextEvaluation(now)
		if err != nil {
			return err
		}
		earlier(next)
		if wake == 0 {
			<-ctx.Done()
			return ctx.Err()
		}
		delay := time.Until(time.Unix(wake, 0))
		if delay < 100*time.Millisecond {
			delay = 100 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// deliver posts one occurrence and reports whether the receiver acknowledged
// it. Any other result leaves the delivery intent in place.
func deliver(ctx context.Context, client *http.Client, endpoint, token string, delivery Delivery) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(delivery.Body))
	if err != nil {
		return false
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", delivery.ID)
	request.Header.Set("User-Agent", fmt.Sprintf("powerfarm-heartime/0 (attempt %d)", delivery.Attempts+1))
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<16))
	return response.StatusCode >= 200 && response.StatusCode < 300
}
