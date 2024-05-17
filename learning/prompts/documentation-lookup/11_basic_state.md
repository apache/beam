Prompt:
What is a state in Apache Beam?

Response:
[State](https://beam.apache.org/documentation/basics/#state-and-timers) in Apache Beam is an API that allows for managing per-key state in element-wise operations like [`ParDo`](https://beam.apache.org/documentation/programming-guide/#pardo) or `Map`. It enables fine-grained control over aggregations, particularly in situations where [windowing](https://beam.apache.org/documentation/programming-guide/#windowing) and [triggers](https://beam.apache.org/documentation/programming-guide/#triggers) are insufficient.

State is [scoped](https://beam.apache.org/documentation/programming-guide/#state-and-timers) to the current processing key within a window, meaning each key has its own state. State is particularly useful for complex aggregations across multiple elements.

Apache Beam provides several [types of state](https://beam.apache.org/documentation/programming-guide/#types-of-state) that can be used for different use cases, including scalar and collection values:
- `ValueState`
- `BagState`
- `SetState`
- `MapState`
- `OrderedListState`
- `CombiningState`

The per-key state needs to be garbage-collected because the increasing size of the state can negatively impact performance. For more information about common garbage collection strategies, see [Garbage collecting state](https://beam.apache.org/documentation/programming-guide/#garbage-collecting-state).

For more information, read the community blog post about [Stateful Processing](https://beam.apache.org/blog/stateful-processing/).

For additional examples of using state in Apache Beam, see [State and Timers examples](https://beam.apache.org/documentation/programming-guide/#state-timers-examples).
