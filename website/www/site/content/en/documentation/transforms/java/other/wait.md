---
title: "Wait.On"
---

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

# Wait.On

`Wait.On` is a transform that delays the processing of a main `PCollection` until one or more other `PCollections` (signals) have finished processing. This is useful for enforcing ordering or dependencies between different parts of a pipeline, especially when some outputs interact with external systems (such as writing to a database).

When you apply `Wait.On`, the elements of the main `PCollection` will not be processed until all the specified signal `PCollections` have completed. In streaming mode, this is enforced per window: the corresponding window of each waited-on `PCollection` must be complete before elements are passed through.

## Examples

```java
// Example 1: Basic usage
PCollection<String> main = ...;
PCollection<Void> signal = ...;

PCollection<String> result = main.apply(Wait.on(signal));

// Example 2: Using multiple signals
PCollection<String> main = ...;
PCollection<Void> signal1 = ...;
PCollection<Void> signal2 = ...;

PCollection<String> result = main.apply(Wait.on(signal1, signal2));

// Example 3: Streaming mode with windowing
PCollection<String> main = ...;
PCollection<Void> signal = ...;

PCollection<String> result = main
    .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5))))
    .apply(Wait.on(signal));
```

## Related transforms
* [Flatten](/documentation/transforms/java/other/flatten) merges multiple `PCollection` objects into a single logical `PCollection`.
* [Window](/documentation/transforms/java/other/window) logically divides or groups elements into finite windows.
* [WithTimestamps](/documentation/transforms/java/elementwise/withtimestamps) assigns timestamps to elements in a collection.
