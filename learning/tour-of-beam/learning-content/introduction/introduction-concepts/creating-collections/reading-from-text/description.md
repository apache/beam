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
### Reading from text file

You use one of the Beam-provided I/O adapters to read from an external source. The adapters vary in their exact usage, but all of them read from some external data source and return a `PCollection` whose elements represent the data records in that source.

Each data source adapter has a Read transform; to read, you must apply that transform to the Pipeline object itself.

`TextIO.Read` , for example, reads from an external text file and returns a `PCollection` whose elements are of type String. Each String represents one line from the text file. Here’s how you would apply `TextIO.Read` to your Pipeline to create a `PCollection`:
{{if (eq .Sdk "go")}}
```
func main() {
    ctx := context.Background()

    // First create pipline
    p, s := beam.NewPipelineWithRoot()

    // Now create the PCollection by reading text files. Separate elements will be added for each line in the input file
    input :=  textio.Read(scope, 'gs://some/inputData.txt')

}
```
{{end}}
{{if (eq .Sdk "java")}}
```
public static void main(String[] args) {
    // First create the pipeline
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // Now create the PCollection by reading text files. Separate elements will be added for each line in the input file
    PCollection<String> input =
        pipeline.apply(“King Lear”,TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt")
);

}
```
{{end}}
{{if (eq .Sdk "python")}}
```
# First create pipline
with beam.Pipeline() as p:

    # Now create the PCollection by reading text files. Separate elements will be added for each line in the input file
    (p | beam.io.ReadFromText('gs://some/inputData.txt'))

```
{{end}}
### Playground exercise

In the playground window, you can find an example that reads a king lear poem from the text file stored in the Google Storage bucket and fills PCollection with individual lines and then with individual words. Try it out and see what the output is.

One of the differences you will see is that the output is much shorter than the input file itself. This is because the number of elements in the output `PCollection` is limited with the {{if (eq .Sdk "go")}}`top.Largest(s,lines,10,less)`{{end}}{{if (eq .Sdk "java")}}`Sample.fixedSizeGlobally`{{end}}{{if (eq .Sdk "python")}}`beam.combiners.Sample.FixedSizeGlobally(10)`{{end}}  transform. Use Sample.fixedSizeGlobally transform of is another technique you can use to troubleshoot and limit the output sent to the console for debugging purposes in case of large input datasets.

Overview [file](https://storage.googleapis.com/apache-beam-samples/shakespeare/kinglear.txt)