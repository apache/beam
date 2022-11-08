# ParDo one-to-one

ParDo is a Beam transform for generic parallel processing. The ParDo processing paradigm is similar to the “Map” phase of a Map/Shuffle/Reduce-style algorithm: a ParDo transform considers each element in the input PCollection, performs some processing function (your user code) on that element, and emits zero, one, or multiple elements to an output PCollection.

ParDo is useful for a variety of common data processing operations, including:

&#8594; Filtering a data set. You can use ParDo to consider each element in a PCollection and either output that element to a new collection or discard it.

&#8594; Formatting or type-converting each element in a data set. If your input PCollection contains elements that are of a different type or format than you want, you can use ParDo to perform a conversion on each element and output the result to a new PCollection.

&#8594; Extracting parts of each element in a data set. If you have a PCollection of records with multiple fields, for example, you can use a ParDo to parse out just the fields you want to consider into a new PCollection.

&#8594; Performing computations on each element in a data set. You can use ParDo to perform simple or complex computations on every element, or certain elements, of a PCollection and output the results as a new PCollection.

In such roles, ParDo is a common intermediate step in a pipeline. You might use it to extract certain fields from a set of raw input records, or convert raw input into a different format; you might also use ParDo to convert processed data into a format suitable for output, like database table rows or printable strings.

When you apply a ParDo transform, you’ll need to provide user code in the form of a DoFn object. DoFn is a Beam SDK class that defines a distributed processing function.

When you create a subclass of DoFn, note that your subclass should adhere to the Requirements for writing user code for Beam transforms.


### Applying ParDo

Like all Beam transforms, you apply `ParDo` by calling the apply method on the input `PCollection` and passing `ParDo` as an argument, as shown in the following example code:

```
# The input PCollection of Strings.
words = ...

# The DoFn to perform on each element in the input PCollection.

class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [len(element)]



# Apply a ParDo to the PCollection "words" to compute lengths for each word.
word_lengths = words | beam.ParDo(ComputeWordLengthFn())
```

In the example, our input `PCollection` contains `String` values. We apply a `ParDo` transform that specifies a function (`ComputeWordLengthFn`) to compute the length of each string, and outputs the result to a new `PCollection` of `Integer` values that stores the length of each word.

### Creating a DoFn

The `DoFn` object that you pass to `ParDo` contains the processing logic that gets applied to the elements in the input collection. When you use Beam, often the most important pieces of code you’ll write are these `DoFn`s - they’re what define your pipeline’s exact data processing tasks.

> Note: When you create your `DoFn`, be mindful of the Requirements for writing user code for Beam transforms and ensure that your code follows them.

Inside your `DoFn` subclass, you’ll write a method process where you provide the actual processing logic. You don’t need to manually extract the elements from the input collection; the Beam SDKs handle that for you. Your process method should accept an argument element, which is the input element, and return an iterable with its output values. You can accomplish this by emitting individual elements with yield statements. You can also use a return statement with an iterable, like a list or a generator.

```
class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [len(element)]
```

> Note: Whether using a structural `DoFn` type or a functional `DoFn`, they should be registered with beam in an init block. Otherwise they may not execute on distributed runners.

A given `DoFn` instance generally gets invoked one or more times to process some arbitrary bundle of elements. However, Beam doesn’t guarantee an exact number of invocations; it may be invoked multiple times on a given worker node to account for failures and retries. As such, you can cache information across multiple calls to your processing method, but if you do so, make sure the implementation does not depend on the number of invocations.

In your processing method, you’ll also need to meet some immutability requirements to ensure that Beam and the processing back-end can safely serialize and cache the values in your pipeline. Your method should meet the following requirements:

&#8594; You should not in any way modify the element argument provided to the process method, or any side inputs.
&#8594; Once you output a value using yield or return, you should not modify that value in any way.

### Accessing additional parameters in your DoFn

In addition to the element, Beam will populate other parameters to your DoFn’s process method. Any combination of these parameters can be added to your process method in any order.

**Timestamp**: To access the timestamp of an input element, add a keyword parameter default to DoFn.TimestampParam. For example:

```
import apache_beam as beam

class ProcessRecord(beam.DoFn):

  def process(self, element, timestamp=beam.DoFn.TimestampParam):
     # access timestamp of element.
     pass
```

**Window**: To access the window an input element falls into, add a keyword parameter default to `DoFn.WindowParam`. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the process method will be invoked multiple time for the element, once for each window.

```
import apache_beam as beam

class ProcessRecord(beam.DoFn):

  def process(self, element, window=beam.DoFn.WindowParam):
     # access window e.g. window.end.micros
     pass
```

**PaneInfo**: When triggers are used, Beam provides a `DoFn.PaneInfoParam` object that contains information about the current firing. Using `DoFn.PaneInfoParam` you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.

```
import apache_beam as beam

class ProcessRecord(beam.DoFn):

  def process(self, element, pane_info=beam.DoFn.PaneInfoParam):
     # access pane info, e.g. pane_info.is_first, pane_info.is_last, pane_info.timing
     pass
```

**Timer and State**: In addition to aforementioned parameters, user defined Timer and State parameters can be used in a stateful `DoFn`. Timers and States are explained in more detail in the Timely (and Stateful) Processing with Apache Beam blog post.

```
class StatefulDoFn(beam.DoFn):
  """An example stateful DoFn with state and timer"""

  BUFFER_STATE_1 = BagStateSpec('buffer1', beam.BytesCoder())
  BUFFER_STATE_2 = BagStateSpec('buffer2', beam.VarIntCoder())
  WATERMARK_TIMER = TimerSpec('watermark_timer', TimeDomain.WATERMARK)

  def process(self,
              element,
              timestamp=beam.DoFn.TimestampParam,
              window=beam.DoFn.WindowParam,
              buffer_1=beam.DoFn.StateParam(BUFFER_STATE_1),
              buffer_2=beam.DoFn.StateParam(BUFFER_STATE_2),
              watermark_timer=beam.DoFn.TimerParam(WATERMARK_TIMER)):

    # Do your processing here
    key, value = element
    # Read all the data from buffer1
    all_values_in_buffer_1 = [x for x in buffer_1.read()]

    if StatefulDoFn._is_clear_buffer_1_required(all_values_in_buffer_1):
        # clear the buffer data if required conditions are met.
        buffer_1.clear()

    # add the value to buffer 2
    buffer_2.add(value)

    if StatefulDoFn._all_condition_met():
      # Clear the timer if certain condition met and you don't want to trigger
      # the callback method.
      watermark_timer.clear()

    yield element

  @on_timer(WATERMARK_TIMER)
  def on_expiry_1(self,
                  timestamp=beam.DoFn.TimestampParam,
                  window=beam.DoFn.WindowParam,
                  key=beam.DoFn.KeyParam,
                  buffer_1=beam.DoFn.StateParam(BUFFER_STATE_1),
                  buffer_2=beam.DoFn.StateParam(BUFFER_STATE_2)):
    # Window and key parameters are really useful especially for debugging issues.
    yield 'expired1'

  @staticmethod
  def _all_condition_met():
      # some logic
      return True

  @staticmethod
  def _is_clear_buffer_1_required(buffer_1_data):
      # Some business logic
      return True
```


### Description for example

There are `PCollection` elements at the input. the `applyTransform()` function will return the elements multiplied by 10.