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

The AfterProcessingTime trigger operates on processing time. For example, the AfterProcessingTime.pastFirstElementInPane() trigger emits a window after a certain amount of processing time has passed since data was received. The processing time is determined by the system clock, rather than the data element’s timestamp.

The AfterProcessingTime trigger is useful for triggering early results from a window, particularly a window with a large time frame such as a single global window.

### Composite trigger types

Beam includes the following composite triggers:

* You can add additional early firings or late firings to `AfterWatermark.pastEndOfWindow` via `.withEarlyFirings` and `.withLateFirings`.
* `Repeatedly.forever` specifies a trigger that executes forever. Any time the trigger’s conditions are met, it causes a window to emit results and then resets and starts over. It can be useful to combine `Repeatedly.forever` with `.orFinally` to specify a condition that causes the repeating trigger to stop.
* `AfterEach.inOrder` combines multiple triggers to fire in a specific sequence. Each time a trigger in the sequence emits a window, the sequence advances to the next trigger.
* `AfterFirst` takes multiple triggers and emits the first time any of its argument triggers is satisfied. This is equivalent to a logical OR operation for multiple triggers.
* `AfterAll` takes multiple triggers and emits when all of its argument triggers are satisfied. This is equivalent to a logical AND operation for multiple triggers.
* `orFinally` can serve as a final condition to cause any trigger to fire one final time and never fire again.

### Composition with AfterWatermark

* Some of the most useful composite triggers fire a single time when Beam estimates that all the data has arrived (i.e. when the watermark passes the end of the window) combined with either, or both, of the following:
* Speculative firings that precede the watermark passing the end of the window to allow faster processing of partial results.
* Late firings that happen after the watermark passes the end of the window, to allow for handling late-arriving data

You can express this pattern using `AfterWatermark`. For example, the following example trigger code fires on the following conditions:

* On Beam’s estimate that all the data has arrived (the watermark passes the end of the window)
* Any time late data arrives, after a ten-minute delay
* After two days, we assume no more data of interest will arrive, and the trigger stops executing
```
.apply(Window
      .configure()
      .triggering(AfterWatermark
           .pastEndOfWindow()
           .withLateFirings(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(10))))
      .withAllowedLateness(Duration.standardDays(2)));
```