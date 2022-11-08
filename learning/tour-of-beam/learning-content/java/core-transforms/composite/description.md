# Composite transforms

Transforms can have a nested structure, where a complex transform performs multiple simpler transforms (such as more than one `ParDo`, `Combine`, `GroupByKey`, or even other composite transforms). These transforms are called composite transforms. Nesting multiple transforms inside a single composite transform can make your code more modular and easier to understand.

### An example composite transform

The CountWords transform in the WordCount example program is an example of a composite transform. CountWords is a PTransform subclass that consists of multiple nested transforms.

In its expand method, the CountWords transform applies the following transform operations:

It applies a ParDo on the input `PCollection` of text lines, producing an output PCollection of individual words.

It applies the Beam SDK library transform Count on the PCollection of words, producing a `PCollection` of key/value pairs. Each key represents a word in the text, and each value represents the number of times that word appeared in the original data.

```
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }
```

> Note: Because Count is itself a composite transform, CountWords is also a nested composite transform.

### Creating a composite transform

To create your own composite transform, create a subclass of the `PTransform` class and override the expand method to specify the actual processing logic. You can then use this transform just as you would a built-in transform from the Beam SDK.

For the `PTransform` class type parameters, you pass the `PCollection` types that your transform takes as input, and produces as output. To take multiple PCollections as input, or produce multiple PCollections as output, use one of the multi-collection types for the relevant type parameter.

The following code sample shows how to declare a `PTransform` that accepts a `PCollection` of Strings for input, and outputs a `PCollection` of `Integers`:

```
  static class ComputeWordLengths
    extends PTransform<PCollection<String>, PCollection<Integer>> {
    ...
  }
```

Within your `PTransform` subclass, you’ll need to override the expand method. The expand method is where you add the processing logic for the `PTransform`. Your override of expand must accept the appropriate type of input `PCollection` as a parameter, and specify the output PCollection as the return value.

The following code sample shows how to override expand for the ComputeWordLengths class declared in the previous example:

```
  static class ComputeWordLengths
      extends PTransform<PCollection<String>, PCollection<Integer>> {
    @Override
    public PCollection<Integer> expand(PCollection<String>) {
      ...
      // transform logic goes here
      ...
    }
```

As long as you override the expand method in your `PTransform` subclass to accept the appropriate input `PCollection`(s) and return the corresponding output `PCollection`(s), you can include as many transforms as you want. These transforms can include core transforms, composite transforms, or the transforms included in the Beam SDK libraries.

Your composite transform’s parameters and return value must match the initial input type and final return type for the entire transform, even if the transform’s intermediate data changes type multiple times.

Note: The expand method of a `PTransform` is not meant to be invoked directly by the user of a transform. Instead, you should call the apply method on the PCollection itself, with the transform as an argument. This allows transforms to be nested within the structure of your pipeline.

### Description for example 

The class `ExtractAndMultiplyNumbers()` is inherited from `PTransform`. It consists of two operations. Parsing integers and multiplication by 10.