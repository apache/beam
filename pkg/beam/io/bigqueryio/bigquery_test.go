package bigqueryio

import "testing"

func TestNewQualifiedTableName(t *testing.T) {
	tests := []struct {
		Name string
		Exp  QualifiedTableName
	}{
		{"a:b.c", QualifiedTableName{Project: "a", Dataset: "b", Table: "c"}},
		{"foo.com:a:b.c", QualifiedTableName{Project: "foo.com:a", Dataset: "b", Table: "c"}},
	}

	for _, test := range tests {
		actual, err := NewQualifiedTableName(test.Name)
		if err != nil {
			t.Errorf("NewQualifiedTableName(%v) failed: %v", test.Name, err)
		}
		if actual != test.Exp {
			t.Errorf("NewQualifiedTableName(%v) = %v, want %v", test.Name, actual, test.Exp)
		}
	}
}
