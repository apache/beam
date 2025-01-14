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
{{if (eq .Sdk "go")}}
While `beam.ParDo` always produces an output `PCollection`, your `DoFn` can produce any number of additional output `PCollection`s, or even none at all. If you choose to have multiple outputs, your `DoFn` needs to be called with the `ParDo` function that matches the number of outputs. `beam.ParDo2` for two output `PCollection`s, `beam.ParDo3` for three and so on until `beam.ParDo7`. If you need more, you can use `beam.ParDoN` which will return a `[]beam.PCollection`.

### Tags for multiple outputs

The Go SDK doesn’t use output tags, and instead uses positional ordering for multiple output `PCollection`s.

```
// beam.ParDo3 returns PCollections in the same order as
// the emit function parameters in processWords.
// Now below has the output of s, above has the output of processWords, and marked has the output of words
below, above, marked := beam.ParDo3(s, processWords, words)

// processWordsMixed uses both a standard return and an emitter function.
// The standard return produces the first PCollection from beam.ParDo2,
// and the emitter produces the second PCollection.
// Now below has the output of s, above has the output of processWords, and marked has the output of words
length, mixedMarked := beam.ParDo2(s, processWordsMixed, words)
```

### Emitting to multiple outputs in your DoFn

Call emitter functions as needed to produce 0 or more elements for its matching `PCollection`. The same value can be emitted with multiple emitters. As normal, do not mutate values after emitting them from any emitter.

All emitters should be registered using a generic `register.EmitterX[...]` function. This optimizes runtime execution of the emitter.

`DoFn`s can also return a single element via the standard return. The standard return is always the first PCollection returned from `beam.ParDo`. Other emitters output to their own `PCollection`s in their defined parameter order.

```
// processWords is a DoFn that has 3 output PCollections. The emitter functions
// are matched in positional order to the PCollections returned by beam.ParDo3.
func processWords(word string, emitBelowCutoff, emitAboveCutoff, emitMarked func(string)) {
	const cutOff = 5
	if len(word) < cutOff {
		emitBelowCutoff(word)
	} else {
		emitAboveCutoff(word)
	}
	if isMarkedWord(word) {
		emitMarked(word)
	}
}
```

### Accessing additional parameters in your DoFn

In addition to the element, Beam will populate other parameters to your DoFn’s `ProcessElement` method. Any combination of these parameters can be added to your process method in a standard order.

**context.Context**: To support consolidated logging and user defined metrics, a `context.Context` parameter can be requested. Per Go conventions, if present it’s required to be the first parameter of the `DoFn` method.

```
func MyDoFn(ctx context.Context, word string) string { ... }
```

**Timestamp**: To access the timestamp of an input element, add a `beam.EventTime` parameter before the element. For example:

```
func MyDoFn(ts beam.EventTime, word string) string { ... }
```

**Window**: To access the window an input element falls into, add a `beam.Window` parameter before the element. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the `ProcessElement` method will be invoked multiple time for the element, once for each window. Since `beam.Window` is an interface it’s possible to type assert to the concrete implementation of the window. For example, when fixed windows are being used, the window is of type `window.IntervalWindow`.

```
func MyDoFn(w beam.Window, word string) string {
  iw := w.(window.IntervalWindow)
  ...
}
```

**PaneInfo**: When triggers are used, Beam provides `beam.PaneInfo` object that contains information about the current firing. Using `beam.PaneInfo` you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.

```
func extractWordsFn(pn beam.PaneInfo, line string, emitWords func(string)) {
	if pn.Timing == typex.PaneEarly || pn.Timing == typex.PaneOnTime {
		// ... perform operation ...
	}
	if pn.Timing == typex.PaneLate {
		// ... perform operation ...
	}
	if pn.IsFirst {
		// ... perform operation ...
	}
	if pn.IsLast {
		// ... perform operation ...
	}

	words := strings.Split(line, " ")
	for _, w := range words {
		emitWords(w)
	}
}
```
{{end}}
{{if (eq .Sdk "java")}}
While `ParDo` always outputs the main output of `PCollection` (as a return value from apply), you can also force your `ParDo` to output any number of additional `PCollection` outputs. If you decide to have multiple outputs, your `ParDo` will return all the `PCollection` outputs (including the main output) combined. This will be useful when you are working with big data or a database that needs to be divided into different collections. You get a combined `PCollectionTuple`, you can use `TupleTag` to get a `PCollection`.

A `PCollectionTuple` is an immutable tuple of heterogeneously typed `PCollection`, "with keys" `TupleTags`. A `PCollectionTuple` can be used as input or output for `PTransform` receiving or creating multiple `PCollection` inputs or outputs, which can be of different types, for example, `ParDo` with multiple outputs.

A `TupleTag` is a typed tag used as the key of a heterogeneously typed tuple, for example `PCollectionTuple`. Its general type parameter allows you to track the static type of things stored in tuples.

### Tags for multiple outputs

```
  ParDo.of(new DoFn<String, String>() {
     public void processElement(@Element String word, MultiOutputReceiver out) {
       if (word.length() <= wordLengthCutOff) {
         // Emit short word to the main output.
         // In this example, it is the output with tag wordsBelowCutOffTag.
         out.get(wordsBelowCutOffTag).output(word);
       } else {
         // Emit long word length to the output with tag wordLengthsAboveCutOffTag.
         out.get(wordLengthsAboveCutOffTag).output(word.length());
       }
       if (word.startsWith("MARKER")) {
         // Emit word to the output with tag markedWordsTag.
         out.get(markedWordsTag).output(word);
       }
     }})).withOutputTags(wordsBelowCutOffTag,
          // Specify the tags for the two additional outputs as a TupleTagList.
                          TupleTagList.of(wordLengthsAboveCutOffTag).and(markedWordsTag)));
```
{{end}}

{{if (eq .Sdk "python")}}
While `ParDo` always produces a main output `PCollection` (as the return value from apply), you can also have your ParDo produce any number of additional output `PCollection`s. If you choose to have multiple outputs, your `ParDo` returns all the output `PCollections` (including the main output) bundled together.

### Tags for multiple outputs

```
# To emit elements to multiple output PCollections, invoke with_outputs() on the ParDo, and specify the
# expected tags for the outputs. with_outputs() returns a DoOutputsTuple object. Tags specified in
# with_outputs are attributes on the returned DoOutputsTuple object. The tags give access to the
# corresponding output PCollections.


results = (
  input | beam.ParDo(ProcessWords(), cutoff_length=2, marker='x').with_outputs('above_cutoff_lengths','marked strings',main='below_cutoff_strings'))
  below = results.below_cutoff_strings
  above = results.above_cutoff_lengths
  marked = results['marked strings']  # indexing works as well


# The result is also iterable, ordered in the same order that the tags were passed to with_outputs(),
# the main tag (if specified) first.


below, above, marked = (input | beam.ParDo(ProcessWords(), cutoff_length=2, marker='x')
                                .with_outputs('above_cutoff_lengths','marked strings',main='below_cutoff_strings'))
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

results = input | beam.FlatMap(even_odd).with_outputs()

evens = results.even
odds = results.odd
tens = results[None]  # the undeclared main output
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The `applyTransform()` accepts a list of integers and outputs two `PCollections`: one `PCollection` above 100 and second below 100.

You can also work with strings:
{{if (eq .Sdk "go")}}
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
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> input = pipeline.apply(Create.of("Apache Beam is an open source unified programming model","To define and execute data processing pipelines","Go SDK"));

PCollection<String> words = input
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))));

PCollectionTuple outputTuple = applyTransform(words, upperCaseTag, lowerCaseTag);
```

Change `applyTransform`:
```
static PCollectionTuple applyTransform(
            PCollection<String> input, TupleTag<String> upperCase,
            TupleTag<String> lowerCase) {

            return input.apply(ParDo.of(new DoFn<String, String>() {

            @ProcessElement
            public void processElement(@Element String element, MultiOutputReceiver out) {
                if (element.equals(element.toLowerCase())) {
                    // First PCollection
                    out.get(lowerCase).output(element);
                } else {
                    // Additional PCollection
                    out.get(upperCase).output(element);
                }
            }

    }).withOutputTags(lowerCase, TupleTagList.of(upperCase)));
}
```
{{end}}

{{if (eq .Sdk "python")}}
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
{{end}}