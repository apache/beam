Prompt:
What is a batched `DoFn` in Apache Beam?

Response:
In Apache Beam, a [batched `DoFn`](https://beam.apache.org/documentation/programming-guide/#batched-dofns) is a specialized type of [`DoFn`](https://beam.apache.org/documentation/programming-guide/#pardo) that operates on batches of logical elements within a [`PCollection`](https://beam.apache.org/documentation/basics/#pcollection). Unlike regular `DoFn` functions, which process each element individually, batched `DoFn` functions enable the efficient processing of multiple elements together as batches.

Batched `DoFn` functions are currently only available in the Apache Beam Python SDK. They leverage powerful Python vectorized libraries, such as [NumPy](https://numpy.org/), [SciPy](https://scipy.org/), and [pandas](https://pandas.pydata.org/), which are designed for analysis and computing at scale.

Batched `DoFn` functions are particularly useful when dealing with large amounts of data, allowing for parallel and optimized processing. Additionally, you can chain multiple batched `DoFn` functions together to create a more efficient data processing pipeline.

To implement a batched `DoFn`, you define a [`process_batch`](https://beam.apache.org/releases/pydoc/current/_modules/apache_beam/transforms/core.html#DoFn.process_batch) method instead of the typical [`process`](https://beam.apache.org/releases/pydoc/current/_modules/apache_beam/transforms/core.html#DoFn.process) method used in a regular `DoFn`. The `process_batch` method takes a batch of elements as input and produces a batch of elements as an output. It is important to note that batches must have a single set of timing properties (event time, window, etc.) for every logical element within the batch; batches cannot span multiple timestamps.
For some `DoFn` functions, you can provide both a batched and an element-wise implementation of your desired logic by defining both `process` and `process_batch` methods.

Here is a simple example of a flexible `DoFn` with both batched and element-wise implementations:

```python
class MultiplyByTen(beam.DoFn):
  def process(self, element: np.int64) -> Iterator[np.int64]:
    yield element * 10

  def process_batch(self, batch: np.ndarray) -> Iterator[np.ndarray]:
    yield batch * 10
```

In this example, when `MultiplyByTen` is applied to a `PCollection`, Apache Beam recognizes that `np.ndarray` is an acceptable batch type to use in conjunction with `np.int64` elements. The `np.int64` type represents the individual element. The `process` method multiplies an element by ten, yielding a single element. The `np.ndarray` type represents the batch. The `process_batch` method multiplies each element in the batch by ten, yielding a single batch. During pipeline execution, Apache Beam will automatically select the best implementation based on the context.

By default, Apache Beam implicitly buffers elements and creates batches on the input side, then explodes batches back into individual elements on the output side. However, if batched `DoFn` functions with equivalent types are chained together, this batch creation and explosion process is skipped, and the batches are passed through for more efficient processing.

Here is an example with chained `DoFn` functions of equivalent types:

```python
(p | beam.Create([1, 2, 3, 4]).with_output_types(np.int64)
   | beam.ParDo(MultiplyByTen()) # Implicit buffering and batch creation
   | beam.ParDo(MultiplyByTen()) # Batches passed through
   | beam.ParDo(MultiplyByTen()))
```

In this example, the `PTransform.with_output_types` method sets the element-wise typehint for the output. Thus, when the `MultiplyByTen` class is applied to a `PCollection`, Apache Beam recognizes that `np.ndarray` is an acceptable batch type to use in conjunction with `np.int64` elements.

The `process_batch` method is expected to produce batched outputs, while the `process` method should produce individual elements. You can customize this production logic with the `@beam.DoFn.yields_elements` and `@beam.DoFn.yields_batches` decorators.

For more details, including advanced usage and code samples, you can refer to the [Apache Beam documentation on batched `DoFn` type](https://beam.apache.org/documentation/programming-guide/#batched-dofn-types).
