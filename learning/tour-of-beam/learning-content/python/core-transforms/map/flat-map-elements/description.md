# FlatMap elements

It works like `Map elements` , but inside the logic you can do complex operations like dividing the list into separate elements and processing

```
with beam.Pipeline() as p:

    (p | beam.Create(['Apache Beam', 'Unified Batch and Streaming'])
     | beam.FlatMap(lambda sentence: sentence.split())
     | LogElements())
```

### Description for example

At the input, the elements of the `PCollection` are represented as strings. The `beam.FlatMap()` returns a list of words that make up the sentence.