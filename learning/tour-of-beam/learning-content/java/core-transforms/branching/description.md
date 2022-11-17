<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Branching PCollections

It’s important to understand that transforms do not consume `PCollection`s; instead, they consider each individual element of a `PCollection` and create a new `PCollection` as output. This way, you can do different things to different elements in the same `PCollection`.

### Multiple transforms process the same PCollection

You can use the same `PCollection` as input for multiple transforms without consuming the input or altering it.

The pipeline reads its input (first names represented as strings) from a database table and creates a `PCollection` of table rows. Then, the pipeline applies multiple transforms to the same `PCollection`. Transform A extracts all the names in that `PCollection` that start with the letter ‘A’, and Transform B extracts all the names in that `PCollection` that start with the letter ‘B’. Both transforms A and B have the same input `PCollection`.

You can use two transforms applied to a single `PCollection`.

The following example code applies two transforms to a single input collection.

```
PCollection<String> dbRowCollection = ...;

PCollection<String> aCollection = dbRowCollection.apply("aTrans", ParDo.of(new DoFn<String, String>(){
  @ProcessElement
  public void processElement(ProcessContext c) {
    if(c.element().startsWith("A")){
      c.output(c.element());
    }
  }
}));

PCollection<String> bCollection = dbRowCollection.apply("bTrans", ParDo.of(new DoFn<String, String>(){
  @ProcessElement
  public void processElement(ProcessContext c) {
    if(c.element().startsWith("B")){
      c.output(c.element());
    }
  }
}));
```

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

Accepts a `PCollection` consisting of strings. Without modification, it returns a new "PCollection". In this case, one `PCollection` includes elements in uppercase. The other `PCollection' stores inverted elements.

You can use the branching method for strings:

```
PCollection<String> input = pipeline.apply(Create.of("Apache", "Beam" ,"is", "an" ,"open", "source" ,"unified","programming" ,"model","To", "define", "and" ,"execute", "data" ,"processing" ,"pipelines","Go", "SDK"));

PCollection<String> aCollection = input.apply("aTrans", ParDo.of(new DoFn<String, String>(){
  @ProcessElement
  public void processElement(ProcessContext c) {
    if(c.element().toLowerCase().startsWith("a")){
      c.output(c.element());
    }
  }
}));
```

Have you also noticed the order in which the collection items are displayed in the console? Why is that? You can also run the example several times to see if the output remains the same or changes.