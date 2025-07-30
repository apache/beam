---
title: "WaitOn"
---

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

# WaitOn

`WaitOn` returns a `PCollection` with the contents identical to the input `PCollection`, but delays the downstream processing until one or more other `PCollections` (signals) have finished processing. This is useful for enforcing ordering or dependencies between different parts of a pipeline, especially when some outputs interact with external systems (such as writing to a database).

When you apply `WaitOn`, the elements of the main `PCollection` will not be emitted for downstream processing until the computations required to produce the specified signal `PCollections` have completed. In streaming mode, this is enforced per window: the corresponding window of each waited-on `PCollection` must close before elements are passed through.

## Examples

```python
import time
import apache_beam as beam
from apache_beam.transforms.util import WaitOn

# Example 1: Basic usage
with beam.Pipeline(options=pipeline_options) as p:
  main = p | 'CreateMain' >> beam.Create([1, 2, 3])
  signal = (
      p | 'CreateSignal' >> beam.Create(['a', 'b'])
      | 'ProcessSignal' >> beam.Map(lambda x: print(f"Processing signal element: {x}") or time.sleep(2)))
  # Wait for 'signal' to complete before processing 'main'
  result = main | 'WaitOnSignal' >> WaitOn(signal)
  # Print each result to logs.
  result | 'PrintExample1' >> beam.Map(lambda x: print(f"Example 1 Final Output: {x}"))

# Example 2: Using multiple signals
with beam.Pipeline(options=pipeline_options) as p:
  main = p | 'CreateMain' >> beam.Create(['item1', 'item2', 'item3'])
  signal1 = (
      p | 'CreateSignal_A' >> beam.Create(['setup_db'])
      | 'ProcessSignal_A' >> beam.Map(lambda x: print("Signal A: Setting up database...") or time.sleep(1)))
  signal2 = (
      p | 'CreateSignal_B' >> beam.Create(['load_config'])
      | 'ProcessSignal_B' >> beam.Map(lambda x: print("Signal B: Loading config...") or time.sleep(3)))
  # Wait for both 'signal1' and 'signal2' to complete before processing 'main'
  result = main | 'WaitOnSignals' >> WaitOn(signal1, signal2)
  # Print each result to logs.
  result | 'PrintExample2' >> beam.Map(lambda x: print(f"Example 2 Final Output: {x.upper()}_READY"))
```

## Related transforms
* [Flatten](/documentation/transforms/python/other/flatten) merges multiple `PCollection` objects into a single logical `PCollection`.
* [WindowInto](/documentation/transforms/python/other/windowinto) logically divides or groups elements into finite windows.
* [Reshuffle](/documentation/transforms/python/other/reshuffle) redistributes elements between workers.
