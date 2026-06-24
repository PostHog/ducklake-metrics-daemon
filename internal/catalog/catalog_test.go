// catalog/catalog_test.go covers pure-function behavior in the package.
// Catalog.Query + Catalog.Stat + Catalog.Open need a real Postgres and
// are deferred to integration tests; the unit tests here pin the bits
// that are easy to break by hand: DSN escaping (a misquote here means
// passwords with special chars produce silent connect failures).
package catalog

import (
	"strings"
	"testing"
)

func TestDSNHappy(t *testing.T) {
	c := Conn{Host: "h.example.com", Port: 5432, Database: "db", Username: "u", Password: "p"}
	got := c.DSN()
	for _, want := range []string{
		"host='h.example.com'",
		"port=5432",
		"dbname='db'",
		"user='u'",
		"password='p'",
		"sslmode=require",
		"connect_timeout=10",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("DSN missing %q; got: %s", want, got)
		}
	}
}

func TestDSNPasswordSpecialChars(t *testing.T) {
	// Real-world passwords from secret managers often contain quotes,
	// backslashes, spaces. libpq kv-form requires single-quote
	// wrapping with backslash escaping; without it the parser
	// silently truncates or rejects.
	cases := []struct {
		name string
		pw   string
		want string // substring of password=… in the DSN
	}{
		{"single quote", `pa'ss`, `password='pa\'ss'`},
		{"backslash", `pa\ss`, `password='pa\\ss'`},
		{"backslash quote", `p\\'a`, `password='p\\\\\'a'`},
		{"space", `pa ss`, `password='pa ss'`},
		{"empty", ``, `password=''`},
		{"unicode", `päss`, `password='päss'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Conn{Host: "h", Port: 5432, Database: "d", Username: "u", Password: tc.pw}
			got := c.DSN()
			if !strings.Contains(got, tc.want) {
				t.Errorf("DSN doesn't contain %q\nfull: %s", tc.want, got)
			}
		})
	}
}

func TestDSNHostnameWithSpaces(t *testing.T) {
	// Pathological but legal-ish input. Should still quote correctly.
	c := Conn{Host: "host with space", Port: 5432, Database: "d", Username: "u", Password: "p"}
	got := c.DSN()
	if !strings.Contains(got, "host='host with space'") {
		t.Errorf("host not properly quoted: %s", got)
	}
}

func TestQuoteKVEdgeCases(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "''"},
		{"plain", "'plain'"},
		{"'", `'\''`},
		{`\`, `'\\'`},
		{`'\`, `'\'\\'`},
		// Whitespace inside a quoted value is libpq-legal; just round-trip.
		{"tab\tinside", "'tab\tinside'"},
		{"newline\ninside", "'newline\ninside'"},
		{"a=b", "'a=b'"},
		{"=", "'='"},
	}
	for _, tc := range cases {
		got := quoteKV(tc.in)
		if got != tc.want {
			t.Errorf("quoteKV(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestQuoteKVNullByteSurvives documents current behavior: quoteKV
// passes null bytes through unmodified. pgx/libpq will reject these at
// connect time. If we ever decide to validate at config-load (likely
// safer than discovering it at first-connect), this test moves to
// config and asserts an error there.
func TestQuoteKVNullByteSurvives(t *testing.T) {
	got := quoteKV("a\x00b")
	if got != "'a\x00b'" {
		t.Errorf("quoteKV stripped or rejected null byte: %q", got)
	}
}
