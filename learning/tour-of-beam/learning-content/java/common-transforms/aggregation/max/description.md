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

# Max

Max provides a variety of different transforms for computing the maximum values in a collection, either globally or for each key.

You can find the global maximum value from the ```PCollection``` by using ```Max.doublesGlobally()```

```
PCollection<Integer> numbers = pipeline.apply(Create.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
PCollection<Double> max = input.apply(Max.doublesGlobally());
```

Output

```
10
```

You can use ```Max.integersPerKey()``` to calculate the maximum Integer associated with each unique key (which is of type String).

```
PCollection<KV<String, Integer>> input = pipeline.apply(
    Create.of(KV.of("🥕", 3),
              KV.of("🥕", 2),
              KV.of("🍆", 1),
              KV.of("🍅", 4),
              KV.of("🍅", 5),
              KV.of("🍅", 3)));
PCollection<KV<String, Integer>> maxPerKey = input.apply(Max.integersPerKey());
```

Output

```
KV{🍅, 5}
KV{🥕, 3}
KV{🍆, 1}
```

You can find the full code of this example in the playground window, which you can run and experiment with.

`Max.doublesGlobally` returns the maximum element from the `PCollection`. If you replace the `integers input` with this `map input`:

```
PCollection<KV<Integer, Integer>> input = pipeline.apply(
    Create.of(KV.of(1, 11),
    KV.of(1, 36),
    KV.of(2, 91),
    KV.of(3, 33),
    KV.of(3, 11),
    KV.of(4, 33)));
```

And replace `Max.doublesGlobally` on `Max.integersPerKey` it will output the maximum numbers by key. It is also necessary to replace the generic type:

```
PCollection<KV<Integer, Integer>> output = applyTransform(numbers);
```

```
static PCollection<KV<Integer, Integer>> applyTransform(PCollection<KV<Integer, Integer>> input) {
        return input.apply(Max.integersPerKey());
    }
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.