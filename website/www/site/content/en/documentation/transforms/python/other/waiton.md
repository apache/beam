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

`WaitOn` delays the processing of a main `PCollection` until one or more other `PCollections` (signals) have finished processing. This is useful for enforcing ordering or dependencies between different parts of a pipeline, especially when some outputs interact with external systems (such as writing to a database).

When you apply `WaitOn`, the elements of the main `PCollection` will not be processed until all the specified signal `PCollections` have completed. In streaming mode, this is enforced per window: the corresponding window of each waited-on `PCollection` must be complete before elements are passed through.

## Examples

```python
import apache_beam as beam
from apache_beam.transforms.util import WaitOn

# Example 1: Basic usage
with beam.Pipeline() as p:
    main = p | 'CreateMain' >> beam.Create([1, 2, 3])
    side = p | 'CreateSide' >> beam.Create(['a', 'b', 'c'])

    result = main | 'WaitOnSide' >> WaitOn(side)
    result | beam.Map(print)

# Example 2: Using multiple signals
with beam.Pipeline() as p:
    main = p | 'CreateMain' >> beam.Create([1, 2, 3])
    side1 = p | 'CreateSide1' >> beam.Create(['a', 'b', 'c'])
    side2 = p | 'CreateSide2' >> beam.Create(['x', 'y', 'z'])

    result = main | 'WaitOnSides' >> WaitOn(side1, side2)
    result | beam.Map(print)

# Example 3: Streaming mode with windowing
with beam.Pipeline() as p:
    main = (p | 'CreateMain' >> beam.Create([1, 2, 3])
             | "ApplyWindow" >> beam.WindowInto(FixedWindows(5 * 60)))
    side = (p | 'CreateSide' >> beam.Create(['a', 'b', 'c'])
             | "ApplyWindow" >> beam.WindowInto(FixedWindows(5 * 60)))

    result = main | 'WaitOnSignal' >> WaitOn(side)
    result | beam.Map(print)
```

## Related transforms
* [Flatten](/documentation/transforms/python/other/flatten) merges multiple `PCollection` objects into a single logical `PCollection`.
* [WindowInto](/documentation/transforms/python/other/windowinto) logically divides or groups elements into finite windows.
* [Reshuffle](/documentation/transforms/python/other/reshuffle) redistributes elements between workers.
