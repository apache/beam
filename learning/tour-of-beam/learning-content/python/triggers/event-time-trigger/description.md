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

### Event time triggers

The `AfterWatermark` trigger operates on event time. The `AfterWatermark` trigger emits the contents of a window after the watermark passes the end of the window, based on the timestamps attached to the data elements. The watermark is a global progress metric, and is Beam’s notion of input completeness within your pipeline at any given point. AfterWatermark.pastEndOfWindow() only fires when the watermark passes the end of the window.

In addition, you can configure triggers that fire if your pipeline receives data before or after the end of the window.

The following example shows a billing scenario, and uses both early and late firings:

```
AfterWatermark(
    early=AfterProcessingTime(delay=1 * 60), late=AfterCount(1))
```

### Default trigger

The default trigger for a `PCollection` is based on event time, and emits the results of the window when the Beam’s watermark passes the end of the window, and then fires each time late data arrives.

However, if you are using both the default windowing configuration and the default trigger, the default trigger emits exactly once, and late data is discarded. This is because the default windowing configuration has an allowed lateness value of 0. See the Handling Late Data section for information about modifying this behavior.

### Processing time triggers

The AfterProcessingTime trigger operates on processing time. For example, the AfterProcessingTime.pastFirstElementInPane() trigger emits a window after a certain amount of processing time has passed since data was received. The processing time is determined by the system clock, rather than the data element’s timestamp.

The AfterProcessingTime trigger is useful for triggering early results from a window, particularly a window with a large time frame such as a single global window.

### Data-driven triggers

Beam provides one data-driven trigger, `AfterPane.elementCountAtLeast()` . This trigger works on an element count; it fires after the current pane has collected at least N elements. This allows a window to emit early results (before all the data has accumulated), which can be particularly useful if you are using a single global window.

It is important to note that if, for example, you specify `.elementCountAtLeast(50)` and only 32 elements arrive, those 32 elements sit around forever. If the 32 elements are important to you, consider using composite triggers to combine multiple conditions. This allows you to specify multiple firing conditions such as “fire either when I receive 50 elements, or every 1 second”.

### Setting a trigger

When you set a windowing function for a `PCollection` by using the Window transform, you can also specify a trigger.

You set the trigger(s) for a `PCollection` by invoking the method `.triggering()` on the result of your `Window.into()` transform. This code sample sets a time-based trigger for a `PCollection`, which emits results one minute after the first element in that window has been processed. The last line in the code sample, `.discardingFiredPanes()`, sets the **window’s accumulation mode**.
```
pcollection | WindowInto(
    FixedWindows(1 * 60),
    trigger=AfterProcessingTime(1 * 60),
    accumulation_mode=AccumulationMode.DISCARDING)
```