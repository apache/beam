Prompt:
What is a `PCollection` in Apache Beam?

Response:
A `PCollection` in Apache Beam is an unordered bag of elements. Each `PCollection` is a potentially distributed, homogeneous data set or data stream, and is owned by the specific `Pipeline` object for which it is created. It’s the primary data structure used in Apache Beam pipelines to handle large-scale data processing in batch and streaming modes.

The following example shows how to create a `PCollection` using the `Create` transform:

```python
import apache_beam as beam

with beam.Pipeline() as pipeline:
  pcollection = pipeline | beam.Create([...])  # Create a PCollection
```

A `PCollection` can either be bounded or unbounded, making it versatile for different data source types. Bounded `PCollection` objects represent a finite data set, such as files or databases, ideal for batch processing. Unbounded `PCollection` objects, on the other hand, represent data streams that continuously grow over time, such as real-time event logs, suitable for stream processing.

Beam’s computational patterns and transforms are focused on situations where distributed data-parallel computation is required. Therefore, a `PCollection` has the following key characteristics:
   - All elements must be of the same type (with support of structured types).
   - Every `PCollection` has a coder, which is a specification of the binary format of the elements.
   - Elements are immutable. They can't be altered after creation.
   - Random access to individual elements of the collection isn't allowed.
   - Beam encodes each element for distributed processing.

For more information about `PCollection` objects in Apache Beam and usage patterns, see the 'Beam Programming Guide: PCollections' section in the Apache Beam documentation.
