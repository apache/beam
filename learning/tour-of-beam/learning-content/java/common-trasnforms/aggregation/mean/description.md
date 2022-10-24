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

# Mean

You can use Mean transforms to compute the arithmetic mean of the elements in a collection or the mean of the values associated with each key in a collection of key-value pairs.

```Mean.globally()``` returns a transformation that returns a collection whose content is the average of the elements of the input collection. If there are no elements in the input collection, 0 is returned.

```
PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Double> mean = numbers.apply(Mean.globally());
```

Output

```
5.5
```


```Mean.perKey()``` returns a transform that returns a collection containing an output element mapping each distinct key in the input collection to the mean of the values associated with that key in the input collection.

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Double>> meanPerKey = input.apply(Mean.perKey());
```

Output

```
KV{🍆, 1.0}
KV{🥕, 2.5}
KV{🍅, 4.0}
```

You can find the full code of this example in the playground window, which you can run and experiment with.

`Mean.globally` returns the mean from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Mean.globally` on `Mean.perKey` it will output the means by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Double>> output = applyTransform(input);
```

```
static PCollection<KV<Integer, Double>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Mean.perKey());
    }
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.