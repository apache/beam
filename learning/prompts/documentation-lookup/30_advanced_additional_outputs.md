Prompt:
What are additional outputs in Apache Beam?

Response:
In Apache Beam, additional outputs, also known as tagged or side outputs, refer to one or multiple extra [`PCollection`](https://beam.apache.org/documentation/basics/#pcollection) objects produced by a single [`ParDo`](https://beam.apache.org/documentation/programming-guide/#pardo) transform in addition to the main output `PCollection`. The `ParDo` transform has the flexibility to produce any number of additional output `PCollection` objects and return them bundled together with the main output `PCollection`.

Additional outputs serve as a mechanism to implement [pipeline branching](https://beam.apache.org/documentation/pipelines/design-your-pipeline/#branching-pcollections). You can use them when there is a need to split the output of a single transform into several `PCollection` objects or produce outputs in different formats. Additional outputs become particularly beneficial when a transform’s computation per element is time-consuming because they enable transforms to process each element in the input `PCollection` just once.

Producing additional outputs requires [tagging](https://beam.apache.org/documentation/programming-guide/#output-tags) each output `PCollection` with a unique identifier, which is then used to [emit](https://beam.apache.org/documentation/programming-guide/#multiple-outputs-dofn) elements to the corresponding output.

In the Apache Beam Java SDK, you can implement additional outputs by creating a `TupleTag` object to identify each collection produced by the `ParDo` transform. After specifying the `TupleTag` objects for each of the outputs, the tags are passed to the `ParDo` using the `.withOutputTags` method. You can find a sample Apache Beam Java pipeline that applies one transform to output two `PCollection` objects in the [Branching `PCollection`s](https://beam.apache.org/documentation/pipelines/design-your-pipeline/#a-single-transform-that-produces-multiple-outputs) section in the Apache Beam documentation.

The following Java code implements two additional output `PCollection` objects for string and integer values in addition to the main output `PCollection` of strings:

```java
// Input PCollection that contains strings.
  PCollection<String> input = ...;
// Output tag for the main output PCollection of strings.
final TupleTag<String> mainOutputTag = new TupleTag<String>(){};
// Output tag for the additional output PCollection of strings.
final TupleTag<String> additionalOutputTagString = new TupleTag<Integer>(){};
// Output tag for the additional output PCollection of integers.
final TupleTag<Integer> additionalOutputTagIntegers = new TupleTag<Integer>(){};

PCollectionTuple results = input.apply(ParDo
          .of(new DoFn<String, String>() {
            // DoFn continues here.
            ...
          })
          // Specify the tag for the main output.
          .withOutputTags(mainOutputTag,
          // Specify the tags for the two additional outputs as a TupleTagList.
                          TupleTagList.of(additionalOutputTagString)
                                      .and(additionalOutputTagIntegers)));

```

The `processElement` method can emit elements to the main output or any additional output by invoking the output method on the `MultiOutputReceiver` object. The output method takes the tag of the output and the element to be emitted as arguments.

```java
public void processElement(@Element String word, MultiOutputReceiver out) {
       if (condition for main output) {
         // Emit element to main output
         out.get(mainOutputTag).output(word);
       } else {
         // Emit element to additional string output
         out.get(additionalOutputTagString).output(word);
       }
       if (condition for additional integer output) {
         // Emit element to additional integer output
         out.get(additionalOutputTagIntegers).output(word.length());
       }
     }
```

In the Apache Beam Python SDK, you can implement additional outputs by invoking the `with_outputs()` method on the `ParDo` and specifying the expected tags for the multiple outputs.

The following Python code demonstrates how to implement additional outputs for a `ParDo` transform that outputs two `PCollection` objects of strings and integers in addition to the main output `PCollection` of strings:

```python
class SplitLinesToWordsFn(beam.DoFn):

  # These tags will be used to tag the outputs of this DoFn.
  OUTPUT_TAG_SHORT_WORDS = 'tag_short_words'
  OUTPUT_TAG_CHARACTER_COUNT = 'tag_character_count'

  def process(self, element):
    # yield a count (integer) to the OUTPUT_TAG_CHARACTER_COUNT tagged collection.
    yield pvalue.TaggedOutput(self.OUTPUT_TAG_CHARACTER_COUNT, len(element))

    words = re.findall(r'[A-Za-z\']+', element)
    for word in words:
      if len(word) <= 3:
        # yield word as an output to the OUTPUT_TAG_SHORT_WORDS tagged collection.
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_SHORT_WORDS, word)
      else:
        # yield word to add it to the main collection.
        yield word
```

The method returns a `DoOutputsTuple` object, with the specified tags serving as attributes that provide `ParDo` with access to the corresponding output `PCollection` objects.

```python
with beam.Pipeline(options=pipeline_options) as p:

    lines = p | ReadFromText(known_args.input)

    # with_outputs allows accessing the explicitly tagged outputs of a DoFn.
    split_lines_result = (
        lines
        | beam.ParDo(SplitLinesToWordsFn()).with_outputs(
            SplitLinesToWordsFn.OUTPUT_TAG_SHORT_WORDS,
            SplitLinesToWordsFn.OUTPUT_TAG_CHARACTER_COUNT,
            main='words'))

    # split_lines_result is an object of type DoOutputsTuple
    words, _, _ = split_lines_result
    short_words = split_lines_result[SplitLinesToWordsFn.OUTPUT_TAG_SHORT_WORDS]
    character_count = split_lines_result.tag_character_count
```

Refer to the [multiple output `ParDo`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/multiple_output_pardo.py) for the complete example code.

For more information about additional outputs and sample pipelines demonstrating their use, you can refer to the Apache Beam documentation on:
* [Tagging multiple outputs](https://beam.apache.org/documentation/programming-guide/#output-tags)
* [Emitting elements to multiple outputs](https://beam.apache.org/documentation/programming-guide/#multiple-outputs-dofn)
* [Accessing additional parameters in your `DoFn`](https://beam.apache.org/documentation/programming-guide/#other-dofn-parameters)
* [Branching `PCollection`s](https://beam.apache.org/documentation/pipelines/design-your-pipeline/#branching-pcollections)
