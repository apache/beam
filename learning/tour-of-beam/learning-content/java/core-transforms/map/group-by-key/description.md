 # GroupByKey

```GroupByKey``` is a Beam transform for processing collections of key/value pairs. It’s a parallel reduction operation, analogous to the Shuffle phase of a Map/Shuffle/Reduce-style algorithm. The input to ```GroupByKey``` is a collection of key/value pairs that represents a multimap, where the collection contains multiple pairs that have the same key, but different values. Given such a collection, you use GroupByKey to collect all of the values associated with each unique key.

```GroupByKey``` is a good way to aggregate data that has something in common. For example, if you have a collection that stores records of customer orders, you might want to group together all the orders from the same postal code (wherein the “key” of the key/value pair is the postal code field, and the “value” is the remainder of the record).

Let’s examine the mechanics of GroupByKey with a simple example case, where our data set consists of words from a text file and the line number on which they appear. We want to group together all the line numbers (values) that share the same word (key), letting us see all the places in the text where a particular word appears.

Our input is a ```PCollection``` of key/value pairs where each word is a key, and the value is a line number in the file where the word appears. Here’s a list of the key/value pairs in the input collection:

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

GroupByKey gathers up all the values with the same key and outputs a new pair consisting of the unique key and a collection of all of the values that were associated with that key in the input collection. If we apply GroupByKey to our input collection above, the output collection would look like this:

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
```

Thus, GroupByKey represents a transform from a multimap (multiple keys to individual values) to a uni-map (unique keys to collections of values).

Using GroupByKey is straightforward:

```
// The input PCollection.
PCollection<KV<String, String>> mapped = ...;

// Apply GroupByKey to the PCollection mapped.
// Save the result as the PCollection reduced.
PCollection<KV<String, Iterable<String>>> reduced = mapped.apply(GroupByKey.<String, String>create());
```

### GroupByKey and unbounded PCollections

If you are using unbounded ```PCollections```, you must use either non-global windowing or an aggregation trigger in order to perform a ```GroupByKey``` or ```CoGroupByKey```. This is because a bounded ```GroupByKey``` or CoGroupByKey must wait for all the data with a certain key to be collected, but with unbounded collections, the data is unlimited. Windowing and/or triggers allow grouping to operate on logical, finite bundles of data within the unbounded data streams.

If you do apply ```GroupByKey``` or ```CoGroupByKey``` to a group of unbounded ```PCollections``` without setting either a non-global windowing strategy, a trigger strategy, or both for each collection, Beam generates an IllegalStateException error at pipeline construction time.

When using ```GroupByKey``` or ```CoGroupByKey``` to group ```PCollections``` that have a windowing strategy applied, all of the PCollections you want to group must use the same windowing strategy and window sizing. For example, all of the collections you are merging must use (hypothetically) identical 5-minute fixed windows, or 4-minute sliding windows starting every 30 seconds.

If your pipeline attempts to use ```GroupByKey``` or CoGroupByKey to merge ```PCollections``` with incompatible windows, Beam generates an IllegalStateException error at pipeline construction time.

### Description for example

A list of strings is provided. The `applyTransform()` method implements grouping by the first letter of words, which will be a list of words.