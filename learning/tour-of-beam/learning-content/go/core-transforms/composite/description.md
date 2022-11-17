<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Composite transforms

Transforms can have a nested structure, where a complex transform performs multiple simpler transforms (such as more than one `ParDo`, `Combine`, `GroupByKey`, or even other composite transforms). These transforms are called composite transforms. Nesting multiple transforms inside a single composite transform can make your code more modular and easier to understand.

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

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

An input consists of a sentence. The first conversion divides the entire sentence into letters excluding spaces. The second conversion counts how many times a letter occurs.

You can split PCollection by words using `split`:

```
func extractWords(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, func(line string, emit func(string)){
    words := strings.Split(line, " ")
		for _, k := range words {
			word := string(k)
			if word != " " {
				emit(word)
			}
		}
	}, input)
}
```

You can use other transformations you can replace `Count` with a `Filter` to output words starting with **p**:

```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	s = s.Scope("CountCharacters")
	words := extractNonSpaceCharacters(s, input)
	return filter.Include(s, words, func(word string) bool {
    		return strings.HasPrefix(word, "p")
    })
}
```


Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.
