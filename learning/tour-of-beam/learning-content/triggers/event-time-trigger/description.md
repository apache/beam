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

Event time trigger is a trigger in Apache Beam that fires based on the timestamps of the elements in a pipeline, as opposed to the current processing time. Event time triggers are used in windowing operations to specify when a window should be closed and its elements should be emitted.

For example, consider a pipeline that ingests data from a streaming source with timestamped elements. The event time trigger can be set to fire based on the timestamps of the elements, rather than the processing time when the elements are processed by the pipeline. This ensures that the windows are closed and the elements are emitted based on the actual event time of the elements, rather than the processing time.

The following accumulation modes are available with event time triggers:

`Discarding`: any late data is discarded and only the data that arrives before the trigger fires is processed.

`Accumulating`: late data is included and the trigger fires whenever the trigger conditions are met.