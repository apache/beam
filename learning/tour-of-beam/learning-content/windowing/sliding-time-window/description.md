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

### Sliding time windows

A sliding time window also represents time intervals in the data stream; however, sliding time windows can overlap. For example, each window might capture 60 seconds' worth of data, but a new window starts every 30 seconds. The frequency with which sliding windows begin is called the period. Therefore, our example would have a window duration of 60 seconds and a period of 30 seconds.

Because multiple windows overlap, most elements in a data set will belong to more than one window. This kind of windowing is helpful for taking running data averages; using sliding time windows, you can compute a running average of the past 60 seconds’ worth of data, updated every 30 seconds.

The following example code shows how to apply `Window` to divide a `PCollection` into sliding time windows. Each window is 30 seconds in length, and a new window begins every five seconds:

{{if (eq .Sdk "go")}}
```
slidingWindowedItems := beam.WindowInto(s,
	window.NewSlidingWindows(5*time.Second, 30*time.Second),
	input)
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> input = ...;
    PCollection<String> slidingWindowedItems = input.apply(
        Window.<String>into(SlidingWindows.of(Duration.standardSeconds(30)).every(Duration.standardSeconds(5))));
```
{{end}}
{{if (eq .Sdk "python")}}
```
from apache_beam import window

sliding_windowed_items = (
    input | 'window' >> beam.WindowInto(window.SlidingWindows(30, 5)))
```
{{end}}

### Playground exercise

Because multiple windows overlap, most elements in a data set will belong to more than one window. This kind of windowing is useful for taking running averages of data; using sliding time windows, you can compute a running average of the past 60 seconds’ worth of data, updated every 30 seconds, in our example.

{{if (eq .Sdk "go")}}
```
max := stats.Max(s, windowedData)
mean := stats.Mean(s, windowedData)
min := stats.Min(s, windowedData)
```
{{end}}
{{if (eq .Sdk "java")}}
```
Combine.globally(Max.ofIntegers())
Combine.globally(Mean.ofIntegers())
Combine.globally(Min.ofIntegers())
```
{{end}}
{{if (eq .Sdk "python")}}
```
beam.CombineGlobally.globally(beam.combiners.MaxCombineFn())
beam.CombineGlobally.globally(beam.combiners.MeanCombineFn())
beam.CombineGlobally.globally(beam.combiners.MinCombineFn())
```
{{end}}