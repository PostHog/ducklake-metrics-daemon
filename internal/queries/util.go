package queries

import (
	"os"
	"sort"
)

// Split out so tests can stub the file read without a tempfile.
var readFile = os.ReadFile

func sortByName(qs []Query) {
	sort.Slice(qs, func(i, j int) bool { return qs[i].Name < qs[j].Name })
}
