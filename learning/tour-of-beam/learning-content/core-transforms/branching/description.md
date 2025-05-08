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
{{if (eq .Sdk "go")}}
```
input = ...;

outputA := applyTransformA(s, input)
outputB := applyTransforB(s, input)

func applyTransformA(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, startWithA, input)
}

func applyTransformB(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, startWithB, input)
}

func startWithA(element string) int {
	if(element.startsWith("A")){
      return element;
    }
}

func startWithA(element string) int {
	if(element.startsWith("B")){
      return element;
    }
}
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> input = ...;

PCollection<String> aCollection = input.apply("aTrans", ParDo.of(new DoFn<String, String>(){
  @ProcessElement
  public void processElement(ProcessContext c) {
    if(c.element().startsWith("A")){
      c.output(c.element());
    }
  }
}));

PCollection<String> bCollection = input.apply("bTrans", ParDo.of(new DoFn<String, String>(){
  @ProcessElement
  public void processElement(ProcessContext c) {
    if(c.element().startsWith("B")){
      c.output(c.element());
    }
  }
}));
```
{{end}}

{{if (eq .Sdk "python")}}
```
starts_with_a = input | beam.Filter(lambda x: x.startswith('A'))
starts_with_b = input | beam.Filter(lambda x: x.startswith('B'))
```
{{end}}

### Playground exercise

You can find the full code of this example in the playground window, which you can run and experiment with.

Accepts a `PCollection` consisting of strings. Without modification, it returns a new `PCollection`. In this case, one `PCollection` includes elements in uppercase. The other `PCollection` stores inverted elements.

You can use a different method of branching. Since `applyTransforms` performs 2 conversions, it takes a lot of time. It is possible to convert `PCollection` separately.
{{if (eq .Sdk "go")}}
```
reversed := reverseString(s, input)
toUpper := toUpperString(s, input)
```
{{end}}

{{if (eq .Sdk "java")}}
```
PCollection<String> input = pipeline.apply(Create.of("Apache", "Beam" ,"is", "an" ,"open", "source" ,"unified","programming" ,"model","To", "define", "and" ,"execute", "data" ,"processing" ,"pipelines","Go", "SDK"));

PCollection<String> reverseCollection = input.apply("aTrans", ParDo.of(new DoFn<String, String>(){
  @ProcessElement
  public void processElement(ProcessContext c) {
    c.output(new StringBuilder(c.element()).reverse().toString());
  }
}));

PCollection<String> upperCollection = input.apply("aTrans", ParDo.of(new DoFn<String, String>(){
  @ProcessElement
  public void processElement(ProcessContext c) {
      c.output(c.element().toUpperCase());
  }
}));
```
{{end}}

{{if (eq .Sdk "python")}}
```
reversed = input | reverseString(...)
toUpper = input | toUpperString(...)
```
{{end}}