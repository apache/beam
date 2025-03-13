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
# FlatMap elements

It works like `Map elements` , but inside the logic you can do complex operations like dividing the list into separate elements and processing
{{if (eq .Sdk "java")}}
```
static PCollection<String> applyTransform(PCollection<String> input) {
        return input.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via(sentence -> Arrays.asList(sentence.split(" ")))
        );
    }
```
{{end}}
{{if (eq .Sdk "python")}}
```
with beam.Pipeline() as p:
  (p | beam.Create(['Apache Beam', 'Unified Batch and Streaming'])
     | beam.FlatMap(lambda sentence: sentence.split())
     | LogElements())
```
{{end}}
### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

You can use other types instead of `Integer`:
{{if (eq .Sdk "java")}}
```
PCollection<String> splitWords = input.apply(
    FlatMapElements.into(strings()).via((KV<String, Integer> wordWithCount) -> {
        List<String> words = new ArrayList<>();
        for (int i = 0; i < wordWithCount.getValue(); i++) {
            words.add(wordWithCount.getKey());
        }
        return words;
    }));
```
{{end}}

{{if (eq .Sdk "python")}}
```
words_with_counts = p | 'Create words with counts' >> beam.Create([
  ('Hello', 1), ('World', 2), ('How', 3), ('are', 4), ('you', 5)])

split_words = words_with_counts | 'Split words' >> beam.FlatMap(
  lambda word_with_count: [word_with_count[0]] * word_with_count[1])
```
{{end}}