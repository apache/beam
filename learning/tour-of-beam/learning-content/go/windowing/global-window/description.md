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

### The single global window

By default, all data in a `PCollection` is assigned to the single global window, and late data is discarded. If your data set is of a fixed size, you can use the global window default for your `PCollection`.

You can use the single global window if you are working with an unbounded data set (e.g. from a streaming data source) but use caution when applying aggregating transforms such as `GroupByKey` and `Combine`. The single global window with a default trigger generally requires the entire data set to be available before processing, which is not possible with continuously updating data. To perform aggregations on an unbounded `PCollection` that uses global windowing, you should specify a non-default trigger for that `PCollection`.

If your `PCollection` is bounded (the size is fixed), you can assign all the elements to a single global window. The following example code shows how to set a single global window for a `PCollection`:

```
globalWindowedItems := beam.WindowInto(s,
	window.NewGlobalWindows(),
	items)
```