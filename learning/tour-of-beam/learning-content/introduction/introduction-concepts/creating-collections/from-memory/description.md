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
### Creating PCollection

Now that you know how to create a Beam pipeline and pass parameters into it, it is time to learn how to create an initial `PCollection` and fill it with data.

There are several options:

→ You can create a PCollection of data stored in an in-memory collection class in your driver program.

→ You can also read the data from a variety of external sources such as local or cloud-based files, databases, or other sources using Beam-provided I/O adapters

Through the tour, most of the examples use either a `PCollection` created from in-memory data or data read from one of the cloud buckets "beam-examples" or "dataflow-samples". These buckets contain sample data sets specifically created for educational purposes.

We encourage you to take a look, explore these data sets and use them while learning Apache Beam.

### Creating a PCollection from in-memory data

You can use the Beam-provided Create transform to create a `PCollection` from an in-memory Collection. You can apply Create transform directly to your Pipeline object itself.

The following example code shows how to do this:
{{if (eq .Sdk "go")}}
```
func main() {
    ctx := context.Background()

    // First create pipeline
    p, s := beam.NewPipelineWithRoot()

    //Now create the PCollection using list of strings
    strings := beam.Create(s, "To", "be", "or", "not", "to", "be","that", "is", "the", "question")

    //Create a numerical PCollection
    numbers := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

}
```
{{end}}
{{if (eq .Sdk "java")}}
```
public static void main(String[] args) {
    // First create the pipeline
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // Now create the PCollection using list of strings
    PCollection<String> strings =
        pipeline.apply(
            Create.of("To", "be", "or", "not", "to", "be","that", "is", "the", "question")
        );

    // Create a numerical PCollection
    PCollection<Integer> numbers =
        pipeline.apply(
            Create.of(1,2,3,4,5,6,7,8,9,10)
        );
}
```
{{end}}
{{if (eq .Sdk "python")}}
```
# First create pipeline
with beam.Pipeline() as p:
    # Create a numerical PCollection
    (p | beam.Create(range(1, 11)))

    # Now create the PCollection using list of strings
    (p | beam.Create(['To', 'be', 'or', 'not', 'to', 'be', 'that', 'is', 'the', 'question']))
```
{{end}}

### Playground exercise

You can find the complete code of this example in the playground window you can run and experiment with.

One of the differences you will notice is that it also contains the part to output `PCollection` elements to the console. Don’t worry if you don’t quite understand it, as the concept of `ParDo` transform will be explained later in the course. Feel free, however, to use it in exercises and challenges to explore results.

Do you also notice in what order elements of PCollection appear in the console? Why is that? You can also run the example several times to see if the output stays the same or changes.