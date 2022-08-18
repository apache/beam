# ParDo one-to-many

It works like ParDo one-to-one, but inside the logic you can do complex operations like dividing the list into separate elements and processing

```
func ApplyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, tokenizeFn, input)
}

func tokenizeFn(input string, emit func(out string)) {
	tokens := strings.Split(input, " ")
	for _, k := range tokens {
		emit(k)
	}
}
```