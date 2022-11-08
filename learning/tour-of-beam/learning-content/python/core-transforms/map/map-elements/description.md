### Lightweight DoFns and other abstractions

If your function is relatively straightforward, you can simplify your use of ```ParDo``` by providing a lightweight DoFn in-line, as a lambda function .

Here’s the previous example, ```ParDo``` with ComputeLengthWordsFn, with the ```DoFn``` specified as a lambda function :

```
# The input PCollection of strings.
words = ...

# Apply a lambda function to the PCollection words.
# Save the result as the PCollection word_lengths.

word_lengths = words | beam.FlatMap(lambda word: [len(word)])
```

If your ```ParDo``` performs a one-to-one mapping of input elements to output elements–that is, for each input element, it applies a function that produces exactly one output element, you can use the higher-level ```Map``` transform.
Here’s the previous example using ```Map```:

```
# The input PCollection of string.
words = ...

# Apply a Map with a lambda function to the PCollection words.
# Save the result as the PCollection word_lengths.

word_lengths = words | beam.Map(len)
```

### Description for example 

At the input, the `PCollection` elements are in the form of numbers. The `beam.Map()` returns elements multiplied by 5.