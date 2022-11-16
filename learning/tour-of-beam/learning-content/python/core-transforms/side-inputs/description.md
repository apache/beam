# Side inputs
In addition to the main input `PCollection`, you can provide additional inputs to a `ParDo` transform in the form of side inputs. A side input is an additional input that your DoFn can access each time it processes an element in the input PCollection. When you specify a side input, you create a view of some other data that can be read from within the ParDo transform’s DoFn while processing each element.

Side inputs are useful if your `ParDo` needs to inject additional data when processing each element in the input PCollection, but the additional data needs to be determined at runtime (and not hard-coded). Such values might be determined by the input data, or depend on a different branch of your pipeline.

### Passing side inputs to ParDo

```
# Side inputs are available as extra arguments in the DoFn's process method or Map / FlatMap's callable.
# Optional, positional, and keyword arguments are all supported. Deferred arguments are unwrapped into their
# actual values. For example, using pvalue.AsIteor(pcoll) at pipeline construction time results in an iterable
# of the actual elements of pcoll being passed into each process invocation. In this example, side inputs are
# passed to a FlatMap transform as extra arguments and consumed by filter_using_length.
words = ...

# Callable takes additional arguments.
def filter_using_length(word, lower_bound, upper_bound=float('inf')):
  if lower_bound <= len(word) <= upper_bound:
    yield word

# Construct a deferred side input.
avg_word_len = (
    words
    | beam.Map(len)
    | beam.CombineGlobally(beam.combiners.MeanCombineFn()))

# Call with explicit side inputs.
small_words = words | 'small' >> beam.FlatMap(filter_using_length, 0, 3)

# A single deferred side input.
larger_than_average = (
    words | 'large' >> beam.FlatMap(
        filter_using_length, lower_bound=pvalue.AsSingleton(avg_word_len))
)

# Mix and match.
small_but_nontrivial = words | beam.FlatMap(
    filter_using_length,
    lower_bound=2,
    upper_bound=pvalue.AsSingleton(avg_word_len))


# We can also pass side inputs to a ParDo transform, which will get passed to its process method.
# The first two arguments for the process method would be self and element.


class FilterUsingLength(beam.DoFn):
  def process(self, element, lower_bound, upper_bound=float('inf')):
    if lower_bound <= len(element) <= upper_bound:
      yield element

small_words = words | beam.ParDo(FilterUsingLength(), 0, 3)


```

### Description for example 

At the entrance we have a map whose key is the city of the country value. And we also have a `Person` structure with his name and city. We can compare cities and embed countries in Person.