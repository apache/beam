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

The following example code shows how to apply Window to divide a PCollection into fixed windows, each 60 seconds in length:

```
from apache_beam import window
fixed_windowed_items = (
    items | 'window' >> beam.WindowInto(window.FixedWindows(60)))
```