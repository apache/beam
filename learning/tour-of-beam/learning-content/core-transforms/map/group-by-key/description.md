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
# GroupByKey

`GroupByKey` is a transform that is used to group elements in a `PCollection` by key. The input to `GroupByKey` is a `PCollection` of key-value pairs, where the keys are used to group the elements. The output of `GroupByKey` is a `PCollection` of key-value pairs, where the keys are the same as the input, and the values are lists of all the elements with that key.

Let’s examine the mechanics of `GroupByKey` with a simple example case, where our data set consists of words from a text file and the line number on which they appear. We want to group together all the line numbers (values) that share the same word (key), letting us see all the places in the text where a particular word appears.

Our input is a `PCollection` of key/value pairs where each word is a key, and the value is a line number in the file where the word appears. Here’s a list of the key/value pairs in the input collection:

```
cat, 1
dog, 5
and, 1
jump, 3
tree, 2
cat, 5
dog, 2
and, 2
cat, 9
and, 6
```

`GroupByKey` gathers up all the values with the same key and outputs a new pair consisting of the unique key and a collection of all of the values that were associated with that key in the input collection. If we apply `GroupByKey` to our input collection above, the output collection would look like this:

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
```

Thus, `GroupByKey` represents a transform from a multimap (multiple keys to individual values) to a uni-map (unique keys to collections of values).

{{if (eq .Sdk "go")}}
```
s := beam.NewScope()
input := beam.Create(s, "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog")

length := beam.ParDo(s, func(word string, emit func(int, string)) {
	emit(len(word), word)
}, input)

grouped := beam.GroupByKey(s, length)
```
{{end}}
{{if (eq .Sdk "java")}}
```
// The input PCollection.
PCollection<KV<String, String>> input = ...;

// Apply GroupByKey to the PCollection input.
// Save the result as the PCollection reduced.
PCollection<KV<String, Iterable<String>>> reduced = input.apply(GroupByKey.<String, String>create());
```
{{end}}
{{if (eq .Sdk "python")}}
While all SDKs have a `GroupByKey` transform, using `GroupBy` is generally more natural. The `GroupBy` transform can be parameterized by the name(s) of properties on which to group the elements of the `PCollection`, or a function taking the each element as input that maps to a key on which to do grouping.

```
input = ...
grouped_words = input | beam.GroupByKey()
```
{{end}}

### GroupByKey and unbounded PCollections

If you are using unbounded `PCollections`, you must use either non-global windowing or an aggregation trigger in order to perform a `GroupByKey` or `CoGroupByKey`. This is because a bounded `GroupByKey` or `CoGroupByKey` must wait for all the data with a certain key to be collected, but with unbounded collections, the data is unlimited. Windowing and/or triggers allow grouping to operate on logical, finite bundles of data within the unbounded data streams.

If you do apply `GroupByKey` or `CoGroupByKey` to a group of unbounded `PCollections` without setting either a non-global windowing strategy, a trigger strategy, or both for each collection, Beam generates an `IllegalStateException` error at pipeline construction time.

When using `GroupByKey` or `CoGroupByKey` to group `PCollections` that have a windowing strategy applied, all of the `PCollections` you want to group must use the same windowing strategy and window sizing. For example, all the collections you are merging must use (hypothetically) identical 5-minute fixed windows, or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use `GroupByKey` or `CoGroupByKey` to merge `PCollections` with incompatible windows, Beam generates an `IllegalStateException` error at pipeline construction time.


### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

A list of strings is provided. The `applyTransform()` method implements grouping by the first letter of words, which will be a list of words.

If we have a word with a certain number, but the difference is in the case of the letters, we can convert and group:

{{if (eq .Sdk "go")}}
```
input := beam.ParDo(s, func(_ []byte, emit func(string, int)){
		emit("banana", 2)
		emit("apple", 4)
		emit("lemon", 3)
		emit("Apple", 1)
		emit("Banana", 5)
		emit("Lemon", 2)
}, beam.Impulse(s))
```

If the keys are duplicated, groupByKey collects data and the values will be stored in an array:
```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	kv := beam.ParDo(s, func(word string,count int) (string, int) {
		return strings.ToLower(word),count
    },input)
	return beam.GroupByKey(s, kv)
}
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<KV<String, Integer>> input = pipeline
                .apply(Create.of(
                        KV.of("banana", 2),
                        KV.of("apple", 4),
                        KV.of("lemon", 3),
                        KV.of("Apple", 1),
                        KV.of("Banana", 5),
                        KV.of("Lemon", 2)
                ));
```
{{end}}
{{if (eq .Sdk "python")}}
```
input = p | 'Fruits' >> Create([
    ("banana", 2),
	("apple", 4),
	("lemon", 3),
	("Apple", 1),
	("Banana", 5),
	("Lemon", 2)
])

input
      .apply("Lowercase", ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, Integer> element = c.element();
                        c.output(KV.of(element.getKey().toLowerCase(), element.getValue()));
                    }
                }))
      .apply("GroupByKey", GroupByKey.create());
```

If the keys are duplicated, groupByKey collects data and the values will be stored in an array:
```
class ApplyTransform(PTransform):
    def expand(self, input):
        return (input | 'Lowercase' >> util.Map(lambda word, count: (word.lower(), count))
                | 'GroupByKey' >> util.GroupByKey())
```
{{end}}