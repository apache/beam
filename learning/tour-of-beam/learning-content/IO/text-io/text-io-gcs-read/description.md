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
### Reading google cloud storage file using TextIO

The `TextIO` class in the Apache Beam provides a way to read and write text files from **Google Cloud Storage** **(GCS)** in a pipeline. To read a text file from GCS using TextIO, you can use the Read method and pass in the GCS file path as a string, which starts with "**gs://**" prefix. Here is an example of reading a text file named "**myfile.txt**" from a GCS bucket named "**mybucket**" and printing its contents:

{{if (eq .Sdk "go")}}
```
p, s := beam.NewPipelineWithRoot()
lines := textio.Read(p, "gs://mybucket/myfile.txt")
beam.ParDo(p, func(line string) {
    fmt.Println(line)
}, lines)
if err := p.Run(); err != nil {
    fmt.Printf("Failed to execute job: %v", err)
}
```
{{end}}
{{if (eq .Sdk "java")}}
```
Pipeline pipeline = Pipeline.create();
pipeline.apply(TextIO.read().from("gs://mybucket/myfile.txt"))
 .apply(ParDo.of(new DoFn<String, Void>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
        }
    }));
pipeline.run();
```
{{end}}
{{if (eq .Sdk "python")}}
```
import apache_beam as beam

p = beam.Pipeline()
p | beam.io.ReadFromText('gs://mybucket/myfile.txt') | beam.Map(print)
p.run()
```
{{end}}

### Playground exercise

In the playground window, you can find an example that reads from a text file and outputs individual words found in the text. Can you modify this example to output found words to another file in reverse form?