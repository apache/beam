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

# Count

`Count` provides many transformations for calculating the count of values in a `PCollection`, either globally or for each key.

Counts the number of elements within each aggregation. The Count transform has three varieties:

### Counting all elements in a PCollection

```Count.globally()``` counts the number of elements in the entire PCollection. The result is a collection with a single element.

```
PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Long> output = numbers.apply(Count.globally());
```

Output
```
10
```

### Counting elements for each key

```Count.perKey()``` counts how many elements are associated with each key. It ignores the values. The resulting collection has one output for every key in the input collection.

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Long>> output = input.apply(Count.perKey());
```

Output

```
KV{🥕, 2}
KV{🍅, 3}
KV{🍆, 1}
```

### Counting all unique elements

```Count.perElement()``` counts how many times each element appears in the input collection. The output collection is a key-value pair, containing each unique element and the number of times it appeared in the original collection.

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 3),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Long>> output = input.apply(Count.perElement());
```

Output

```
KV{KV{🍅, 3}, 2}
KV{KV{🥕, 2}, 1}
KV{KV{🍆, 1}, 1}
KV{KV{🥕, 3}, 1}
KV{KV{🍅, 5}, 1}
```

You can find the full code of this example in the playground window, which you can run and experiment with.

`Count` returns the count elements from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Count.globally` on `Count.perKey` it will output the count numbers by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Integer>> output = applyTransform(numbers);
```

```
static PCollection<KV<Integer, Integer>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Count.globally());
    }
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.