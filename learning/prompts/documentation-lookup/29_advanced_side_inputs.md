Prompt:
What are Side inputs in Apache Beam?
Response:
[Side input](https://beam.apache.org/documentation/programming-guide/#side-inputs) is an additional input to a [ParDo transform](https://beam.apache.org/documentation/programming-guide/#pardo) that is not part of the main input `PCollection`. ParDo can access side inputs each time that your `DoFn` processes an element in the input `PCollection`.
Side inputs are useful in scenarios where you need to include additional non hard-coded data at runtime. For example, you might want to include a configuration data or a lookup table in your pipeline. Such data can also depend on a different branch of your pipeline.

In Python SDK side inputs are available as extra arguments to the `DoFn`'s `process` method or `Map`/`FlatMap` callable.

```python
class MyDoFn(beam.DoFn):
  def process(self, element, side_input):
    ...
```

In Java SDK side inputs are passed to your `ParDo` transform by invoking `.withSideInputs`. Inside your `DoFn`, access the side input by using the method `DoFn.ProcessContext.sideInput`.

```java
PCollection<Integer> input = ...;
PCollectionView<Integer> sideInput = ...;
PCollection<Integer> output =
    input.apply(ParDo.of(new DoFn<Integer, Integer>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        Integer sideInputValue = c.sideInput(sideInput);
        ...
      }
    }).withSideInputs(sideInput));
```

Side inputs can also be used for [processing windowed data](https://beam.apache.org/documentation/programming-guide/#side-inputs-windowing). Apache Beam uses the windows for the main input `PCollection` to lookup appropriate window for the side input element. Apache Beam projects the main input window to the side input's window set. Main and side inputs might have identical or different windowing strategies.

For example, if the main input `PCollection` is windowed into 10-minute windows, and the side input `PCollection` is windowed into 1-hour fixed windows, Apache Beam will project the main input window  and select side inputfrom the corresponding 1-hour window.

See [side input pattern](https://beam.apache.org/documentation/patterns/side-inputs/) for more information.
