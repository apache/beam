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
# Additional outputs

While `ParDo` always produces a main output `PCollection` (as the return value from apply), you can also have your ParDo produce any number of additional output `PCollection`s. If you choose to have multiple outputs, your `ParDo` returns all the output `PCollections` (including the main output) bundled together.

### Tags for multiple outputs

```
# To emit elements to multiple output PCollections, invoke with_outputs() on the ParDo, and specify the
# expected tags for the outputs. with_outputs() returns a DoOutputsTuple object. Tags specified in
# with_outputs are attributes on the returned DoOutputsTuple object. The tags give access to the
# corresponding output PCollections.


results = (
    words
    | beam.ParDo(ProcessWords(), cutoff_length=2, marker='x').with_outputs(
        'above_cutoff_lengths',
        'marked strings',
        main='below_cutoff_strings'))
below = results.below_cutoff_strings
above = results.above_cutoff_lengths
marked = results['marked strings']  # indexing works as well


# The result is also iterable, ordered in the same order that the tags were passed to with_outputs(),
# the main tag (if specified) first.


below, above, marked = (words
                        | beam.ParDo(
                            ProcessWords(), cutoff_length=2, marker='x')
                        .with_outputs('above_cutoff_lengths',
                                      'marked strings',
                                      main='below_cutoff_strings'))
```

### Emitting to multiple outputs in your DoFn

```
# Inside your ParDo's DoFn, you can emit an element to a specific output by wrapping the value and the output tag (str).
# using the pvalue.OutputValue wrapper class.
# Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.


class ProcessWords(beam.DoFn):
  def process(self, element, cutoff_length, marker):
    if len(element) <= cutoff_length:
      # Emit this short word to the main output.
      yield element
    else:
      # Emit this word's long length to the 'above_cutoff_lengths' output.
      yield pvalue.TaggedOutput('above_cutoff_lengths', len(element))
    if element.startswith(marker):
      # Emit this word to a different output with the 'marked strings' tag.
      yield pvalue.TaggedOutput('marked strings', element)



# Producing multiple outputs is also available in Map and FlatMap.
# Here is an example that uses FlatMap and shows that the tags do not need to be specified ahead of time.


def even_odd(x):
  yield pvalue.TaggedOutput('odd' if x % 2 else 'even', x)
  if x % 10 == 0:
    yield x

results = numbers | beam.FlatMap(even_odd).with_outputs()

evens = results.even
odds = results.odd
tens = results[None]  # the undeclared main output
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The `applyTransform()` accepts a list of integers at the output two `PCollection` one `PCollection` above 100 and second below 100.

You can also work with strings:

```
input := beam.Create(s, "Apache Beam is an open source unified programming model","To define and execute data processing pipelines","Go SDK")

words := extractWords(s,input)

upperCaseWords, lowerCaseWords := applyTransform(s, words)
```

It is necessary to divide sentences into words. To do this, we use `ParDo`:
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

Change logic `applyTransform`:
```
func applyTransform(s beam.Scope, input beam.PCollection) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, func(element string, upperCaseWords, lowerCaseWords func(string)) {
		if element==strings.Title(element) {
			upperCaseWords(element)
			return
		}
		lowerCaseWords(element)
	}, input)
}
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.