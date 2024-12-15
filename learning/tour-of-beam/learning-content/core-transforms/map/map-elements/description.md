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
# Map elements

If your function is relatively simple, you can simplify the use of `ParDo` by providing a lightweight built-in `DoFn` as an anonymous instance of the internal class.
{{if (eq .Sdk "java")}}
```
PCollection<String> input = ...;

// Apply a ParDo with an anonymous DoFn to the PCollection [input].
// Save the result as the PCollection wordLengths.
PCollection<Integer> wordLengths = input.apply(
  "ComputeWordLengths",                     // the transform name
  ParDo.of(new DoFn<String, Integer>() {    // a DoFn as an anonymous inner class instance
      @ProcessElement
      public void processElement(@Element String word, OutputReceiver<Integer> out) {
        out.output(word.length());
      }
    }));
```

If your `ParDo` performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can use the higher-level `MapElements` transform. `MapElements` can accept an anonymous Java 8 lambda function for additional brevity.

Here’s the previous example using `MapElements` :

```
// The input PCollection.
PCollection<String> input = ...;

// Apply a MapElements with an anonymous lambda function to the PCollection [input].
// Save the result as the PCollection wordLengths.
PCollection<Integer> wordLengths = input.apply(
  MapElements.into(TypeDescriptors.integers())
             .via((String word) -> word.length()));
```
{{end}}

{{if (eq .Sdk "python")}}
Here’s the previous example, `ParDo` with ComputeLengthWordsFn, with the `DoFn` specified as a lambda function :

```
# The input PCollection of strings.
input = ...

# Apply a lambda function to the PCollection input.
# Save the result as the PCollection word_lengths.

word_lengths = input | beam.FlatMap(lambda word: [len(word)])
```

If your `ParDo` performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can use the higher-level `Map` transform.
Here’s the previous example using `Map`:

```
# The input PCollection of string.
input = ...

# Apply a Map with a lambda function to the PCollection input.
# Save the result as the PCollection word_lengths.

word_lengths = input | beam.Map(len)
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

You can use other types instead of `Integer`:
{{if (eq .Sdk "java")}}
```
PCollection<String> input = pipeline.apply(Create.of("Hello", "World", "How", "are", "you"));

PCollection<String> uppercaseWords = input.apply(
    MapElements.into(strings()).via((String word) -> word.toUpperCase()));

uppercaseWords.apply(ParDo.of(new DoFn<String, Void>() {
    @ProcessElement
    public void processElement(ProcessContext c) {
        System.out.println(c.element());
    }
}));
```
{{end}}
{{if (eq .Sdk "python")}}
```
input = p | 'Create words' >> beam.Create(['Hello', 'World', 'How', 'are', 'you'])

uppercase_words = input | 'Convert to uppercase' >> beam.Map(lambda word: word.upper())
```
{{end}}