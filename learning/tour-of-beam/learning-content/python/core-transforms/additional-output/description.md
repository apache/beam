# Additional outputs

While `ParDo` always produces a main output `PCollection` (as the return value from apply), you can also have your ParDo produce any number of additional output `PCollection`s. If you choose to have multiple outputs, your `ParDo` returns all the output `PCollections` (including the main output) bundled together.

### Tags for multiple outputs

```
# To emit elements to multiple output PCollections, invoke with_outputs() on the ParDo, and specify the
# expected tags for the outputs. with_outputs() returns a DoOutputsTuple object. Tags specified in
# with_outputs are attributes on the returned DoOutputsTuple object. The tags give access to the
# corresponding output PCollections.


results = (
    words
    | beam.ParDo(ProcessWords(), cutoff_length=2, marker='x').with_outputs(
        'above_cutoff_lengths',
        'marked strings',
        main='below_cutoff_strings'))
below = results.below_cutoff_strings
above = results.above_cutoff_lengths
marked = results['marked strings']  # indexing works as well


# The result is also iterable, ordered in the same order that the tags were passed to with_outputs(),
# the main tag (if specified) first.


below, above, marked = (words
                        | beam.ParDo(
                            ProcessWords(), cutoff_length=2, marker='x')
                        .with_outputs('above_cutoff_lengths',
                                      'marked strings',
                                      main='below_cutoff_strings')) 
```

### Emitting to multiple outputs in your DoFn

```
# Inside your ParDo's DoFn, you can emit an element to a specific output by wrapping the value and the output tag (str).
# using the pvalue.OutputValue wrapper class.
# Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.


class ProcessWords(beam.DoFn):
  def process(self, element, cutoff_length, marker):
    if len(element) <= cutoff_length:
      # Emit this short word to the main output.
      yield element
    else:
      # Emit this word's long length to the 'above_cutoff_lengths' output.
      yield pvalue.TaggedOutput('above_cutoff_lengths', len(element))
    if element.startswith(marker):
      # Emit this word to a different output with the 'marked strings' tag.
      yield pvalue.TaggedOutput('marked strings', element)



# Producing multiple outputs is also available in Map and FlatMap.
# Here is an example that uses FlatMap and shows that the tags do not need to be specified ahead of time.


def even_odd(x):
  yield pvalue.TaggedOutput('odd' if x % 2 else 'even', x)
  if x % 10 == 0:
    yield x

results = numbers | beam.FlatMap(even_odd).with_outputs()

evens = results.even
odds = results.odd
tens = results[None]  # the undeclared main output
```

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