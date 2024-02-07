Prompt:
What is a Batched DoFn in Apache Beam?
Response:
In Apache Beam, a [Batched DoFn](https://beam.apache.org/documentation/programming-guide/#batched-dofns) is a [DoFn](https://beam.apache.org/documentation/programming-guide/#pardo) that can operate on batches of logical elements. These `DoFns` leverage popular Python vectorized libraries like [NumPy](https://numpy.org/), [SciPy](https://scipy.org/) and [pandas](https://pandas.pydata.org/) to efficiently process batches of elements in a `PCollection` rather than processing each element individually.

Batched `DoFns` are only available in the Apache Beam Python SDK.

You can provide element-wise, batched or both implementation for a `DoFn`

In example below, when `MultiplyByTen` is applied to a `PCollection`, Beam recognizes that `np.ndarray` is an acceptable batch type to use in conjunction with `np.int64` elements.

```python
class MultiplyByTen(beam.DoFn):
  def process(self, element: np.int64) -> Iterator[np.int64]:
    yield element * 10

  def process_batch(self, batch: np.ndarray) -> Iterator[np.ndarray]:
    yield batch * 10
```
Apache Beam runner will select the best implementation during pipeline execution.

Apache Beam implicitly creates and explodes batches at the input and output boundaries. However, if Batched DoFns with equivalent types are chained together, this batch creation and explosion will be skipped making the compose batch transform more efficient.

```python
(p | beam.Create([1, 2, 3, 4]).with_output_types(np.int64)
   | beam.ParDo(MultiplyByTen()) # Implicit buffering and batch creation
   | beam.ParDo(MultiplyByTen()) # Batches passed through
   | beam.ParDo(MultiplyByTen()))
```

Apache Beam assumes that the `process_batch` method will  produce batched outputs and the `process` method will produce individual elements. This can be overridden with the `@beam.DoFn.yields_elements` and` @beam.DoFn.yields_batches` decorators.

Please see [here](https://beam.apache.org/documentation/programming-guide/#batched-dofn-types) for a list of supported element and batch typehints.

Batches must have a single set of timing properties(event time, window) for every logical element in the batch and it's not possible to create batches that span multiple timestamps.
