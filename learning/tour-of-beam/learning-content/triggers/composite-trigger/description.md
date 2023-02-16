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

### Composite triggers

A **composite trigger** in Apache Beam allows you to specify multiple triggers to be used in combination. When any of the triggers fire, the composite trigger will fire. This allows you to combine different types of triggers to create more complex triggering strategies.

{{if (eq .Sdk "go")}}
```
trigger := trigger.AfterAll([]trigger.Trigger{trigger.AfterEndOfWindow().
	EarlyFiring(trigger.AfterProcessingTime().
		PlusDelay(60 * time.Second)).
	LateFiring(trigger.Repeat(trigger.AfterCount(1))),trigger.AfterCount(2)})
```
{{end}}
{{if (eq .Sdk "java")}}
```
Window<String> window = Window.into(FixedWindows.of(Duration.standardMinutes(5)));

        Trigger processingTimeTrigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1));
        Trigger dataDrivenTrigger = AfterPane.elementCountAtLeast(2);

        PCollection<String> windowed = input.apply(window.triggering(AfterAll.of(Arrays.asList(processingTimeTrigger,dataDrivenTrigger))).withAllowedLateness(Duration.ZERO).accumulatingFiredPanes());
```
{{end}}
{{if (eq .Sdk "python")}}
```
event_time_trigger = trigger.AfterWatermark(early=trigger.AfterCount(100),
                                             late=trigger.AfterCount(200))

composite_trigger = trigger.AfterAll(processing_time_trigger,event_time_trigger)
```
{{end}}