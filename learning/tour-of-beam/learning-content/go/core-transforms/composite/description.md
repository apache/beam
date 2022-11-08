# Composite transforms

Transforms can have a nested structure, where a complex transform performs multiple simpler transforms (such as more than one `ParDo`, `Combine`, `GroupByKey`, or even other composite transforms). These transforms are called composite transforms. Nesting multiple transforms inside a single composite transform can make your code more modular and easier to understand.

### An example composite transform

The `CountWords` transform in the WordCount example program is an example of a composite transform. CountWords is a PTransform that consists of multiple nested transforms.

The `CountWords` transform applies the following transform operations:

1. It applies a `ParDo` on the input `PCollection` of text lines, producing an output `PCollection` of individual words.
2. It applies the Beam SDK library transform Count on the PCollection of words, producing a `PCollection` of key/value pairs. Each key represents a word in the text, and each value represents the number of times that word appeared in the original data.

```
// CountWords is a function that builds a composite PTransform
// to count the number of times each word appears.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	// A subscope is required for a function to become a composite transform.
	// We assign it to the original scope variable s to shadow the original
	// for the rest of the CountWords function.
	s = s.Scope("CountWords")

	// Since the same subscope is used for the following transforms,
	// they are in the same composite PTransform.

	// Convert lines of text into individual words.
	words := beam.ParDo(s, extractWordsFn, lines)

	// Count the number of times each word occurs.
	wordCounts := stats.Count(s, words)

	// Return any PCollections that should be available after
	// the composite transform.
	return wordCounts
}
```

> Note: Because Count is itself a composite transform, CountWords is also a nested composite transform.

### Creating a composite transform

To create your own composite `PTransform` call the Scope method on the current pipeline scope variable. Transforms passed this new sub-Scope will be a part of the same composite `PTransform`.

To be able to re-use your Composite, build it inside a normal Go function or method. This function is passed a scope and input PCollections, and returns any output `PCollection`s it produces. Note: Such functions cannot be passed directly to `ParDo` functions.

```
// CountWords is a function that builds a composite PTransform
// to count the number of times each word appears.
func CountWords(s beam.Scope, lines beam.PCollection) beam.PCollection {
	// A subscope is required for a function to become a composite transform.
	// We assign it to the original scope variable s to shadow the original
	// for the rest of the CountWords function.
	s = s.Scope("CountWords")

	// Since the same subscope is used for the following transforms,
	// they are in the same composite PTransform.

	// Convert lines of text into individual words.
	words := beam.ParDo(s, extractWordsFn, lines)

	// Count the number of times each word occurs.
	wordCounts := stats.Count(s, words)

	// Return any PCollections that should be available after
	// the composite transform.
	return wordCounts
}
```

The following code sample shows how to call the CountWords composite PTransform, adding it to your pipeline:

```
lines := ... // a PCollection of strings.

// A Composite PTransform function is called like any other function.
wordCounts := CountWords(s, lines) // returns a PCollection<KV<string,int>>
```

Your composite `PTransform`s can include as many transforms as you want. These transforms can include core transforms, other composite transforms, or the transforms included in the Beam SDK libraries. They can also consume and return as many `PCollection`s as are necessary.

### Description for example 

When entering a sentence of words. The `applyTransform()` implements the main operation and invokes nested logic. In nested logic, we collect characters from a sentence, in the main function return their count.