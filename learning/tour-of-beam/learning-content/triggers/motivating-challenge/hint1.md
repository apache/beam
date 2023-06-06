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

To solve this challenge, you may build a pipeline that consists of the following steps:
{{if (eq .Sdk "go")}}
1. Add composite trigger `trigger := trigger.AfterAll(trigger.AfterCount(10),trigger.AfterEndOfWindow().
   EarlyFiring(trigger.AfterProcessingTime().
   PlusDelay(60 * time.Second)).
   LateFiring(trigger.Repeat(trigger.AfterCount(1))))`
2. Write window `fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(60*time.Second),input,beam.Trigger(trigger), beam.PanesDiscard())`
{{end}}

{{if (eq .Sdk "java")}}
1. Add composite trigger `Trigger dataDrivenTrigger = AfterPane.elementCountAtLeast(2);
   Trigger processingTimeTrigger = AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1));`
2. Write window `PCollection<Double> windowed = rideTotalAmounts.apply(window.triggering(AfterAll.of(Arrays.asList(dataDrivenTrigger,processingTimeTrigger))).withAllowedLateness(Duration.ZERO).accumulatingFiredPanes());`
{{end}}

{{if (eq .Sdk "python")}}
1. Add composite trigger `data_driven_trigger =  trigger.AfterEach(trigger.AfterCount(10))
   processing_time_trigger = trigger.AfterProcessingTime(60)
   composite_trigger = trigger.AfterAll(data_driven_trigger,processing_time_trigger)`
2. Write window `beam.WindowInto(FixedWindows(2),
   trigger=composite_trigger ,
   accumulation_mode=trigger.AccumulationMode.DISCARDING)`
{{end}}
