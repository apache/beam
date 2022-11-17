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
```GroupByKey``` is a Beam transform for processing collections of key/value pairs. It’s a parallel reduction operation, analogous to the Shuffle phase of a Map/Shuffle/Reduce-style algorithm. The input to ```GroupByKey``` is a collection of key/value pairs that represents a multimap, where the collection contains multiple pairs that have the same key, but different values. Given such a collection, you use ```GroupByKey``` to collect all of the values associated with each unique key.

```GroupByKey``` is a good way to aggregate data that has something in common. For example, if you have a collection that stores records of customer orders, you might want to group together all the orders from the same postal code (wherein the “key” of the key/value pair is the postal code field, and the “value” is the remainder of the record).

Let’s examine the mechanics of GroupByKey with a simple example case, where our data set consists of words from a text file and the line number on which they appear. We want to group together all the line numbers (values) that share the same word (key), letting us see all the places in the text where a particular word appears.

Our input is a PCollection of key/value pairs where each word is a key, and the value is a line number in the file where the word appears. Here’s a list of the key/value pairs in the input collection:

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

```GroupByKey``` gathers up all the values with the same key and outputs a new pair consisting of the unique key and a collection of all of the values that were associated with that key in the input collection. If we apply ```GroupByKey``` to our input collection above, the output collection would look like this:

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
```

Thus, GroupByKey represents a transform from a multimap (multiple keys to individual values) to a uni-map (unique keys to collections of values).

While all SDKs have a GroupByKey transform, using GroupBy is generally more natural. The GroupBy transform can be parameterized by the name(s) of properties on which to group the elements of the PCollection, or a function taking the each element as input that maps to a key on which to do grouping.

```
words_and_counts = ...
grouped_words = words_and_counts | beam.GroupByKey()
```

### GroupByKey and unbounded PCollections

If you are using unbounded ```PCollections```, you must use either non-global windowing or an aggregation trigger in order to perform a ```GroupByKey``` or ```CoGroupByKey```. This is because a bounded ```GroupByKey``` or ```CoGroupByKey``` must wait for all the data with a certain key to be collected, but with unbounded collections, the data is unlimited. Windowing and/or triggers allow grouping to operate on logical, finite bundles of data within the unbounded data streams.

If you do apply ```GroupByKey``` or ```CoGroupByKey``` to a group of unbounded ```PCollections``` without setting either a non-global windowing strategy, a trigger strategy, or both for each collection, Beam generates an IllegalStateException error at pipeline construction time.

When using ```GroupByKey``` or ```CoGroupByKey``` to group PCollections that have a windowing strategy applied, all of the ```PCollections``` you want to group must use the same windowing strategy and window sizing. For example, all of the collections you are merging must use (hypothetically) identical 5-minute fixed windows, or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use ```GroupByKey``` or ```CoGroupByKey``` to merge ```PCollections``` with incompatible windows, Beam generates an IllegalStateException error at pipeline construction time.

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

A list of strings is provided. The `applyTransform()` method implements grouping by the first letter of words, which will be a list of words.

If we have a word with a certain number, but the difference is in the case of the letters, we can convert and group:
```
input := beam.ParDo(s, func(_ []byte, emit func(string, int)){
		emit("brazil", 2)
		emit("australia", 4)
		emit("canada", 3)
		emit("Australia", 1)
		emit("Brazil", 5)
		emit("Canada", 2)
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

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.