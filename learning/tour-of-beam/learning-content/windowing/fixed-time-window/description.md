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

### Fixed time windows

The simplest form of windowing is using fixed time windows: given a timestamped `PCollection` which might be continuously updating, each window might capture (for example) all elements with timestamps that fall into a 30-second interval.

A fixed time window represents a consistent duration, non overlapping time interval in the data stream. Consider windows with a 30-second duration: all the elements in your unbounded PCollection with timestamp values from 0:00:00 up to (but not including) 0:00:30 belong to the first window, elements with timestamp values from 0:00:30 up to (but not including) 0:01:00 belong to the second window, and so on.

{{if (eq .Sdk "go")}}
```
fixedWindowedItems := beam.WindowInto(s,
	window.NewFixedWindows(30*time.Second),
	items)
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> input = ...;
    PCollection<String> fixedWindowedItems = input.apply(
        Window.<String>into(FixedWindows.of(Duration.standardSeconds(30))));
```
{{end}}

{{if (eq .Sdk "python")}}
```
from apache_beam import window

fixed_windowed_items = (
    input | 'window' >> beam.WindowInto(window.FixedWindows(30)))
```
{{end}}

### Playground exercise

You can start displaying elements from the beginning but also from the end:

{{if (eq .Sdk "go")}}
You can write your logic inside `MyCombineFn`:
`accumulated := beam.CombinePerKey(s, &MyCombineFn{}, windowed)`
{{end}}

{{if (eq .Sdk "java")}}
```
input.apply(...)
 .apply(Window.<Type>into(FixedWindows.of(Duration.standardMinutes(10))
    .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW))
```
{{end}}

{{if (eq .Sdk "python")}}
```
from apache_beam import window

fixed_windowed_items = (
    input | 'window' >> beam.WindowInto(window.FixedWindows(30),timestamp_combiner=TimestampCombiner.OUTPUT_AT_END)))
```
{{end}}
