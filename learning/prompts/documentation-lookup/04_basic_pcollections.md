Prompt:
What is a PCollection in Apache Beam?
Response:
A `PCollection` in Apache Beam is a core abstraction representing a distributed, multi-element data set or data stream. It’s the primary data structure used in Apache Beam pipelines to handle large-scale data processing in batch and streaming modes.

```python
import apache_beam as beam

with beam.Pipeline() as pipeline:
  pcollection = pipeline | beam.Create([...])  # Create a PCollection
```

A `PCollection` can either be bounded or unbounded, making it versatile for different [data source](https://beam.apache.org/documentation/basics/#pcollection) types. Bounded `PCollections` represent a finite data set, such as files or databases, ideal for batch processing. Unbounded `PCollections`, on the other hand, represent data streams that continuously grow over time, such as real-time event logs, suitable for stream processing.

Beam’s computational patterns and transforms are focused on situations where distributed data-parallel computation is required. Therefore, PCollections has the following key characteristics:
   - All elements must be of the same type (with support of structured types)
   - Every PCollection has a coder, which is a specification of the binary format of the elements.
   - Elements cannot be altered after creation (immutability)
   - No random access to individual elements of the collection
   - Beam encodes each element for distributed processing.

For more in-depth understanding and usage patterns of `PCollection`s in Apache Beam, refer to the [Beam Programming Guide: PCollections](https://beam.apache.org/documentation/programming-guide/#pcollections).
