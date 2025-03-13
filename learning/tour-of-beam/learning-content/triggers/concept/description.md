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

### Triggers

When collecting and grouping data into windows, Beam uses triggers to determine when to emit the aggregated results of each window (referred to as a pane). If you use Beam’s default windowing configuration and default trigger, Beam outputs the aggregated result when it estimates all data has arrived and discards all subsequent data for that window.

You can set triggers for your PCollections to change this default behavior. Beam provides several pre-built triggers that you can set:

* Event time triggers. These triggers operate on the event time, as indicated by the timestamp on each data element. Beam’s default trigger is event time-based.
* Processing time triggers. These triggers operate on the processing time – the time when the data element is processed at any given stage in the pipeline.
* Data-driven triggers. These triggers operate by examining the data as it arrives in each window and firing when that data meets a certain property. Currently, data-driven triggers only support firing after a certain number of data elements.
* Composite triggers. These triggers combine multiple triggers in various ways.

### Handling late data

If you want your pipeline to process data that arrives after the watermark passes the end of the window, you can apply an allowed lateness when you set your windowing configuration. This gives your trigger the opportunity to react to the late data. If allowed lateness is set, the default trigger will emit new results immediately whenever late data arrives.

You set the allowed lateness by using `.withAllowedLateness()` when you set your windowing function:

{{if (eq .Sdk "java")}}
```
PCollection<String> input = ...;
input.apply(Window.<String>into(FixedWindows.of(1, TimeUnit.MINUTES))
                              .triggering(AfterProcessingTime.pastFirstElementInPane()
                                                             .plusDelayOf(Duration.standardMinutes(1)))
                              .withAllowedLateness(Duration.standardMinutes(30));
```
{{end}}
{{if (eq .Sdk "go")}}
```
allowedToBeLateItems := beam.WindowInto(s,
	window.NewFixedWindows(1*time.Minute), pcollection,
	beam.Trigger(trigger.AfterProcessingTime().
		PlusDelay(1*time.Minute)),
	beam.AllowedLateness(30*time.Minute),
)
```
{{end}}
{{if (eq .Sdk "python")}}
```
input = [Initial PCollection]
input | beam.WindowInto(
            FixedWindows(60),
            trigger=AfterProcessingTime(60),
            allowed_lateness=1800) # 30 minutes
     | ...
```
{{end}}

Beam SDK provides various built-in triggers:
* `AfterAll` - the example of composite trigger. It fires when all sub-triggers defined through of(List<Trigger> triggers) method are ready.
* `AfterEach` - the sub-triggers are defined in inOrder(List<Trigger> triggers) method. The sub-trigger are executed in order, one by one.
* `AfterFirst` - executes when at least one of defined sub-triggers fires. As AfterAll trigger, AfterFirst also defines the sub-triggers in of(...) method.
* `AfterPane` - it's an example of data-driven trigger. It uses elementCountAtLeast(int countElems) method to define the minimal number of accumulated items before executing the trigger. It's important to know that even if this threshold is never reached, the trigger can execute for the lower number.
* `AfterProcessingTime` - as the name indicates, it's an example of processing time-based trigger. It defines 2 methods to control trigger firing. The first one is called plusDelayOf(final Duration delay). It defines the interval of time during which new elements are accumulated. The second method, alignedTo(final Duration period, final Instant offset) does the same but in additional it adds specified period to the defined offset. Thus if we define the offset at 2017-01-01 10:00 and we allow the period of 4 minutes, it'll accept the data betweenn 10:00 and 10:04.
* `AfterWatermark` - its method pastEndOfWindow() creates a trigger firing the pane after the end of the window. It also has more fine-grained access because it allows the definition for early results (withEarlyFirings(OnceTrigger earlyFirings), produced before the end of the window) and late results (withLateFirings(OnceTrigger lateFirings), produced after the end of the window and before the end of watermark).
* `DefaultTrigger` - it's the class used by default that is an equivalent to repeatable execution of AfterWatermark trigger.
* `NeverTrigger` - the pane is fired only after the passed window plus allowed lateness delay.
* `OrFinallyTrigger` - it's a special kind of trigger constructed through Trigger's orFinally(OnceTrigger until) method. With the call to this method the main trigger executes until the moment when until trigger is fired.
* `Repeatedly` - helps to execute given trigger repeatedly. The sub-trigger is defined in forever(Trigger repeated) method. That said even if we defined a AfterPane.elementCountAtLeast(2) as a repeatable sub-trigger, it won't stop after the first 2 elements in the pane but will continue the execution for every new 2 items.

### Window accumulation

When you specify a trigger, you must also set the window’s accumulation mode. When a trigger fires, it emits the current contents of the window as a pane. Since a trigger can fire multiple times, the accumulation mode determines whether the system accumulates the window panes as the trigger fires, or discards them.

To set a window to accumulate the panes that are produced when the trigger fires, invoke.accumulatingFiredPanes() when you set the trigger. To set a window to discard fired panes, invoke .discardingFiredPanes().

Let’s look an example that uses a PCollection with fixed-time windowing and a data-based trigger. This is something you might do if, for example, each window represented a ten-minute running average, but you wanted to display the current value of the average in a UI more frequently than every ten minutes. We’ll assume the following conditions:

→ The PCollection uses 10-minute fixed-time windows.
→ The PCollection has a repeating trigger that fires every time 3 elements arrive.

The following diagram shows data events for key X as they arrive in the PCollection and are assigned to windows. To keep the diagram a bit simpler, we’ll assume that the events all arrive in the pipeline in order.

### Accumulating mode

If our trigger is set to accumulating mode, the trigger emits the following values each time it fires. Keep in mind that the trigger fires every time three elements arrive
```
First trigger firing:  [5, 8, 3]
Second trigger firing: [5, 8, 3, 15, 19, 23]
Third trigger firing:  [5, 8, 3, 15, 19, 23, 9, 13, 10]
```

{{if (eq .Sdk "go")}}
```
fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(60*time.Second),words,trigger, beam.PanesAccumulate())
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> windowed = words.apply(window.triggering(trigger).withAllowedLateness(Duration.ZERO).accumulatingFiredPanes());
```
{{end}}

{{if (eq .Sdk "python")}}
```
input | WindowInto(
    FixedWindows(1 * 60),
    trigger=AfterProcessingTime(1 * 60),
    accumulation_mode=AccumulationMode.ACCUMULATING)
```
{{end}}



### Discarding mode

If our trigger is set to discarding mode, the trigger emits the following values on each firing:
```
First trigger firing:  [5, 8, 3]
Second trigger firing:           [15, 19, 23]
Third trigger firing:                         [9, 13, 10]
```

{{if (eq .Sdk "go")}}
```
fixedWindowedItems := beam.WindowInto(s, window.NewFixedWindows(60*time.Second),input,trigger, beam.PanesDiscard())
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> windowed = input.apply(window.triggering(trigger).withAllowedLateness(Duration.ZERO).discardingFiredPanes());
```
{{end}}

{{if (eq .Sdk "python")}}
```
input | WindowInto(
    FixedWindows(1 * 60),
    trigger=AfterProcessingTime(1 * 60),
    accumulation_mode=AccumulationMode.DISCARDING)
```
{{end}}
