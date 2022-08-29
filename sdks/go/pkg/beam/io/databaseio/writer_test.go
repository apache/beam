package databaseio

import "testing"

func TestValueTemplateGenerator(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		generator := &valueTemplateGenerator{"postgres"}
		values := generator.generate(4, 3)
		expected := "($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12)"
		if values != expected {
			t.Fatalf("valueTemplateGenerator failed: %v != %v", values, expected)
		}

		values = generator.generate(0, 10)
		if values != "" {
			t.Fatalf("valueTemplateGenerator failed: %v is not empty", values)
		}
	})

	t.Run("mysql", func(t *testing.T) {
		generator := &valueTemplateGenerator{"mysql"}
		values := generator.generate(4, 3)
		expected := "(?,?,?),(?,?,?),(?,?,?),(?,?,?)"
		if values != expected {
			t.Fatalf("valueTemplateGenerator failed: %v != %v", values, expected)
		}

		values = generator.generate(0, 10)
		if values != "" {
			t.Fatalf("valueTemplateGenerator failed: %v is not empty", values)
		}
	})
}
