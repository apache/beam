package runtime

import "testing"

func TestOptions(t *testing.T) {
	opt := &Options{opt: make(map[string]string)}

	if len(opt.Export().Options) != 0 {
		t.Errorf("fresh map not empty")
	}

	opt.Set("foo", "1")
	opt.Set("foo2", "2")
	opt.Set("/", "3")
	opt.Set("?", "4")

	if v := opt.Get("foo2"); v != "2" {
		t.Errorf("Get(foo2) = %v, want 2", v)
	}

	m := opt.Export()
	if len(m.Options) != 4 {
		t.Errorf("len(%v) = %v, want 4", m, len(m.Options))
	}

	opt.Set("bad", "5")

	opt.Import(m)

	m2 := opt.Export()
	if len(m2.Options) != 4 {
		t.Errorf("len(%v) = %v, want 4", m, len(m.Options))
	}
}
