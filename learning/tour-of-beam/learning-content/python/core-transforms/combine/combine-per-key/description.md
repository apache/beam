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
# PCollection is grouped by key and the numeric values associated with each key
# are averaged into a float.
player_accuracies = ...

avg_accuracy_per_player = (
    player_accuracies
    | beam.CombinePerKey(beam.combiners.MeanCombineFn()))
```