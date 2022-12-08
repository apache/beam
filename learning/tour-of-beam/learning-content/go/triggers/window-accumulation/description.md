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

### Discarding mode

If our trigger is set to discarding mode, the trigger emits the following values on each firing:
```
First trigger firing:  [5, 8, 3]
Second trigger firing:           [15, 19, 23]
Third trigger firing:                         [9, 13, 10]
```