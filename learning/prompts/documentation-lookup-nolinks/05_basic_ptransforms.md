Prompt:
What is a `PTransform` in Apache Beam?

Response:
A `PTransform` (or transform) represents a data processing operation, or a step, in an Apache Beam pipeline. A transform is applied to zero or more `PCollection` objects and produces zero or more `PCollection` objects.

Transforms have the following key characteristics:
1. Versatility: Able to execute a diverse range of operations on `PCollection` objects.
2. Composability: Can be combined to form elaborate data processing pipelines.
3. Parallel execution: Designed for distributed processing, allowing simultaneous execution across multiple workers.
4. Scalability: Able to handle extensive data and suitable for both batch and streaming data.

The Beam SDKs contain different transforms that you can apply to your pipeline’s `PCollection` objects. The following list includes common transform types:
 - Source transforms such as `TextIO.Read` and `Create`. A source transform conceptually has no input.
 - Processing and conversion operations such as `ParDo`, `GroupByKey`, `CoGroupByKey`, `Combine`, and `Count`.
 - Outputting transforms such as `TextIO.Write`.
 - User-defined, application-specific composite transforms.

Transform processing logic is provided in the form of a function object, colloquially referred to as 'user code'. This code is applied to each element of the input `PCollection` (or more than one `PCollection`). The `PCollection` objects can be linked together to create complex data processing sequences.
User code for transforms must satisfy the requirements of the Beam model.

The following example shows how to apply custom user code to a `PCollection` using the `ParDo` transform:

```python
import apache_beam as beam

def SomeUserCode(element):
    # Do something with an element
    return element

with beam.Pipeline() as pipeline:
    input_collection = pipeline | beam.Create([...])
    output_collection = input_collection | beam.ParDo(SomeUserCode())
```
