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

# CombinePerKey

For `Map` collection with a key (for example, using the groupByKey transformation), a common pattern is to combine a collection of values associated with each key into one combined value. Approximately it will look like this:

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
...
```

In the above `PCollection`, each element has a string key (for example, “cat”) and an iterable of integers for its value (in the first element, containing [1, 5, 9]). If our pipeline’s next processing step combines the values (rather than considering them individually), you can combine the iterable of integers to create a single, merged value to be paired with each key. This pattern of a GroupByKey followed by merging the collection of values is equivalent to Beam’s `Combine` `PerKey` transform. The combine function you supply to `Combine` `PerKey` must be an associative reduction function or a subclass of `CombineFn`.

```
// PCollection is grouped by key and the numeric values associated with each key
// are averaged into a float64.
animalCount := ... // PCollection<string,int>

avgAccuracyPerPlayer := stats.MeanPerKey(s, animalCount)

// avgAnimalCount is a PCollection<string,float64>
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

The input data consists of a card (key-value), in this case the key is the player's number, the value of the point won. Combine PerKey groups by key and combine values

You change the numbers to a string, the key of which will be the first letter of the fruit, to the value of the full name of the fruit:

```
input := beam.ParDo(s, func(_ []byte, emit func(string, string)){
		emit("a", "apple")
		emit("o", "orange")
		emit("a", "avocado")
		emit("l", "lemon")
		emit("l", "limes")
	}, beam.Impulse(s))
```

You combine by key by inserting a comma between them:
```
func applyTransform(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.CombinePerKey(s, func(name1, name2 string) string {
        return name1+","+name2
	}, input)
}
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.