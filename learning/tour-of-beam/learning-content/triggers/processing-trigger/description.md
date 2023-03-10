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

### Processing time trigger

Processing time trigger is a trigger in Apache Beam that fires based on the current processing time of the pipeline. Unlike the event time trigger that fires based on the timestamps of the elements, the processing time trigger fires based on the actual time that the elements are processed by the pipeline.

Processing time triggers are used in windowing operations to specify when the window should be closed and its elements should be emitted. The processing time trigger can be set to fire after a fixed interval, after a set of elements have been processed, or when a processing time timer fires.

The following accumulation modes are available with processing time triggers:

`Discarding`: any late data is discarded and only the data that arrives before the trigger fires is processed.

`Accumulating`: late data is included and the trigger fires whenever the trigger conditions are met.

{{if (eq .Sdk "go")}}
```
trigger := beam.Trigger(trigger.AfterProcessingTime().PlusDelay(5 * time.Millisecond))

fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(60*time.Second),input,trigger,
                        beam.AllowedLateness(30*time.Minute),
                        beam.PanesDiscard(),
  )
```
{{end}}
{{if (eq .Sdk "java")}}
```
Trigger trigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1));

PCollection<String> windowed = input.apply(window.triggering(trigger).withAllowedLateness(Duration.ZERO).discardingFiredPanes());
```
{{end}}
{{if (eq .Sdk "python")}}
```
(p | beam.Create(['Hello Beam','It`s trigger'])
   | 'window' >>  beam.WindowInto(FixedWindows(2),
                                                trigger=trigger.AfterProcessingTime(1),
                                                accumulation_mode=trigger.AccumulationMode.DISCARDING) \
   | ...)
```
{{end}}