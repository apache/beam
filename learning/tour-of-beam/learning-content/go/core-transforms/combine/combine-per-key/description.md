# Combining values in a keyed PCollection

After creating a keyed `PCollection` (for example, by using a GroupByKey transform), a common pattern is to combine the collection of values associated with each key into a single, merged value. Drawing on the previous example from GroupByKey, a key-grouped `PCollection` called groupedWords looks like this:

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
...
```

In the above `PCollection`, each element has a string key (for example, “cat”) and an iterable of integers for its value (in the first element, containing [1, 5, 9]). If our pipeline’s next processing step combines the values (rather than considering them individually), you can combine the iterable of integers to create a single, merged value to be paired with each key. This pattern of a `GroupByKey` followed by merging the collection of values is equivalent to Beam’s Combine PerKey transform. The combine function you supply to Combine PerKey must be an associative reduction function or a `CombineFn`.
```
// PCollection is grouped by key and the numeric values associated with each key
// are averaged into a float64.
playerAccuracies := ... // PCollection<string,int>

avgAccuracyPerPlayer := stats.MeanPerKey(s, playerAccuracies)

// avgAccuracyPerPlayer is a PCollection<string,float64>
```