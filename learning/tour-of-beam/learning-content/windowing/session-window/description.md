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

### Session windows

A session window function defines windows containing elements within a specific gap duration of another element. Session windowing applies on a per-key basis and helps process irregularly distributed data with respect to time. For example, a data stream representing user mouse activity may have long periods of idle time interspersed with high concentrations of clicks. If data arrives after the minimum specified gap duration time, this initiates the start of a new window. In addition, it is useful when you want to group related elements based on the time that passed between them rather than on a fixed interval of time.

The following example code shows how to apply `Window` to divide a `PCollection` into session windows, where each session must be separated by a time gap of at least 10 minutes (600 seconds):

{{if (eq .Sdk "go")}}
```
sessionWindowedItems := beam.WindowInto(s,
	window.NewSessions(600*time.Second),
	input)
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> input = ...;
    PCollection<String> sessionWindowedItems = input.apply(
        Window.<String>into(Sessions.withGapDuration(Duration.standardSeconds(600))));
```
{{end}}

{{if (eq .Sdk "python")}}
```
from apache_beam import window

session_windowed_items = (
    input | 'window' >> beam.WindowInto(window.Sessions(10 * 60)))
```
{{end}}

### Playground exercise

To determine when to launch and install a new session window you have to set a trigger:

{{if (eq .Sdk "go")}}
```
sessionDuration := time.Minute * 30

// Apply the session window to the PCollection
sessionWindowed := beam.ParDo(s, func(elm type.T, emit func(type.T)) {
    window := beam.NewWindow(s, beam.SessionsWindowFn(sessionDuration))
    windowed := beam.AddFixedWindows(s, elm, window)
    emit(windowed)
}, input)
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> sessionWindowed = input.apply(
                Window.<String>into(Sessions.withGapDuration(sessionDuration))
                .triggering(sessionTrigger)
                .withAllowedLateness(Duration.ZERO)
                .accumulatingFiredPanes());
```
{{end}}

{{if (eq .Sdk "python")}}
```
session_duration = 30 # minutes
session_window = Window.into(Sessions(session_duration))
session_trigger = AfterWatermark()

session_windowed = input | 'Session Window' >> WindowInto(session_window, triggers=session_trigger)
```
{{end}}