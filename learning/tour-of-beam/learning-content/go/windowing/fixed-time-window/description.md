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

### Windowing

Windowing subdivides a `PCollection` according to the timestamps of its individual elements. Transforms that aggregate multiple elements, such as GroupByKey and Combine, work implicitly on a per-window basis — they process each PCollection as a succession of multiple, finite windows, though the entire collection itself may be of unbounded size.

Some Beam transforms, such as `GroupByKey` and `Combine`, group multiple elements by a common key. Ordinarily, that grouping operation groups all the elements that have the same key within the entire data set. With an unbounded data set, it is impossible to collect all the elements, since new elements are constantly being added and may be infinitely many (e.g. streaming data). If you are working with unbounded PCollections, windowing is especially useful.

### Fixed time windows

The simplest form of windowing is using fixed time windows: given a timestamped `PCollection` which might be continuously updating, each window might capture (for example) all elements with timestamps that fall into a 30-second interval.

A fixed time window represents a consistent duration, non overlapping time interval in the data stream. Consider windows with a 30-second duration: all the elements in your unbounded PCollection with timestamp values from 0:00:00 up to (but not including) 0:00:30 belong to the first window, elements with timestamp values from 0:00:30 up to (but not including) 0:01:00 belong to the second window, and so on.

```
fixedWindowedItems := beam.WindowInto(s,
	window.NewFixedWindows(60*time.Second),
	items)
```