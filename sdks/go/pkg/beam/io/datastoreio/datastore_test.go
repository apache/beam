package datastoreio

import (
	"testing"
	"cloud.google.com/go/datastore"
)

func Test_keyLessThan(t *testing.T) {
	tsts := []struct {
		a      *datastore.Key
		b      *datastore.Key
		expect bool
		name   string
	}{
		{
			name:   "a<b",
			a:      datastore.NameKey("A", "a", nil),
			b:      datastore.NameKey("A", "b", nil),
			expect: true,
		},
		{
			name:   "b>a",
			a:      datastore.NameKey("A", "b", nil),
			b:      datastore.NameKey("A", "a", nil),
			expect: false,
		},
		{
			name:   "a=a",
			a:      datastore.NameKey("A", "a", nil),
			b:      datastore.NameKey("A", "a", nil),
			expect: false,
		},
		{
			name:   "a.a<b",
			a:      datastore.NameKey("A", "a", nil),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "b", nil)),
			expect: true,
		},
		{
			name:   "a.a<a.b",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "b", nil)),
			expect: true,
		},
		{
			name:   "a.b>a.a",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "b", nil)),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			expect: false,
		},
		{
			name:   "a.a=a.a",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			b:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			expect: false,
		},
		{
			name:   "a.a>a",
			a:      datastore.NameKey("A", "a", datastore.NameKey("A", "a", nil)),
			b:      datastore.NameKey("A", "a", nil),
			expect: true,
		},
		{
			name:   "b>b.a",
			a:      datastore.NameKey("A", "b", nil),
			b:      datastore.NameKey("A", "b", datastore.NameKey("A", "a", nil)),
			expect: false,
		},
	}
	for n := range tsts {
		index := n
		t.Run(tsts[index].name, func(t *testing.T) {
			got := keyLessThan(tsts[index].a, tsts[index].b)
			if tsts[index].expect != got {
				t.Fail()
			}
		})
	}
}
