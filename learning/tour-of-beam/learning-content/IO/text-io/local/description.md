### TextIO local file

Apache Beam is a programming model for data processing pipelines that can be executed on a variety of runtime environments, including **Apache Flink**, **Apache Spark**, and **Google Cloud Dataflow**. The `TextIO` class in Apache Beam provides a way to read and write text files in a pipeline. To read a local file using TextIO, you can use the Read method and pass in the file path as a string. Here is an example of reading a local text file named "**myfile.txt**" and printing its contents:
{{if (eq .Sdk "go")}}
```
p := beam.NewPipeline()
lines := textio.Read(p, "myfile.txt")
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
Pipeline p = Pipeline.create();
p.apply(TextIO.read().from("myfile.txt"))
 .apply(ParDo.of(new DoFn<String, Void>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println(c.element());
        }
    }));
p.run();
```
{{end}}
{{if (eq .Sdk "python")}}
```
import apache_beam as beam

p = beam.Pipeline()
lines = p | beam.io.ReadFromText('myfile.txt')
lines | beam.Map(print)
p.run()
```
{{end}}

To write data to a local file, you can use the Write method and pass in the file path as a string. Here is an example of writing a string to a local text file named "**myfile.txt**":

{{if (eq .Sdk "java")}}
```
p := beam.NewPipeline()
s := beam.Create(p, "Hello, World!")
textio.Write(s, "myfile.txt")
if err := p.Run(); err != nil {
    fmt.Printf("Failed to execute job: %v", err)
}
```
{{end}}

{{if (eq .Sdk "java")}}
```
Pipeline p = Pipeline.create();
p.apply(Create.of("Hello, World!"))
 .apply(TextIO.write().to("myfile.txt"));
p.run();
```
{{end}}
{{if (eq .Sdk "python")}}
```
import apache_beam as beam

p = beam.Pipeline()
data = ['Hello, World!', 'Apache Beam']
p | beam.Create(data) | beam.io.WriteToText('myfile.txt')
p.run()
```
{{end}}
It is important to note that the `Read` and `Write` methods only read and write to local file systems and not the distributed file systems like **HDFS**, **GCS**, **S3** etc.