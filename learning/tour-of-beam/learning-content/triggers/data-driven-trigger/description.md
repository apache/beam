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

### Data driven triggers

A **data-driven trigger** is a type of trigger in Apache Beam that fires when specific conditions on the data being processed are met. Unlike processing time triggers, which fire at regular intervals based on the current system time, data-driven triggers are based on the data itself.

For example, a data-driven trigger might fire when a certain number of elements have been processed, or when the values of the elements reach a specific threshold. Data-driven triggers are particularly useful when processing time-sensitive data where it's important to respond to changes in the data as they happen.

Apache Beam provides several options for data-driven triggers, including element count triggers, processing time triggers, and custom triggers. By using a combination of these triggers, you can implement complex processing logic that takes into account both processing time and the state of the data.

{{if (eq .Sdk "go")}}
```
fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(2*time.Second), input,
  		beam.Trigger(trigger.AfterCount(2)),
  		beam.AllowedLateness(30*time.Minute),
      beam.PanesDiscard(),
    )
```
{{end}}
{{if (eq .Sdk "java")}}
```
Trigger trigger = AfterPane.elementCountAtLeast(100);
PCollection<String> windowed = input.apply(window.triggering(trigger).withAllowedLateness(Duration.ZERO).discardingFiredPanes());
```
{{end}}
{{if (eq .Sdk "python")}}
```
(p | beam.Create(['Hello Beam','It`s trigger'])
     | 'window' >> beam.WindowInto(FixedWindows(2),trigger=trigger.AfterCount(2),accumulation_mode=trigger.AccumulationMode.DISCARDING) \
     | ... )
```
{{end}}