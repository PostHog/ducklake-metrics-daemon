// main_test.go covers the package-main helpers that aren't reachable
// from internal/. classifyConnectError in particular is operationally
// critical — it labels every connect failure with a reason that drives
// alerting; getting the classification wrong silently sends operators
// chasing the wrong root cause.
package main

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestClassifyConnectError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"auth 28P01", &pgconn.PgError{Code: "28P01"}, "auth"},
		{"auth 28000", &pgconn.PgError{Code: "28000"}, "auth"},
		{"invalid_database 3D000", &pgconn.PgError{Code: "3D000"}, "invalid_database"},
		{"other postgres code", &pgconn.PgError{Code: "08006"}, "postgres_08006"},
		{"ctx deadline", context.DeadlineExceeded, "timeout"},
		{
			"net dial",
			&net.OpError{Op: "dial", Err: errors.New("connection refused")},
			"dial",
		},
		{
			"net non-dial (e.g. write)",
			&net.OpError{Op: "write", Err: errors.New("broken pipe")},
			"other",
		},
		{
			"DNS error bare",
			&net.DNSError{Err: "no such host", Name: "h.example.com"},
			"dial",
		},
		{"plain unknown", errors.New("???"), "other"},
		{
			"wrapped auth",
			wrap(&pgconn.PgError{Code: "28P01"}),
			"auth",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyConnectError(tc.err)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// wrap embeds an error inside a generic wrapper so we can verify
// classifyConnectError uses errors.As (not direct type assertion).
type wrappedErr struct{ inner error }

func (w wrappedErr) Error() string { return "wrap: " + w.inner.Error() }
func (w wrappedErr) Unwrap() error { return w.inner }

func wrap(e error) error { return wrappedErr{inner: e} }
