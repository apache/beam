### TextIO local file

The Apache Beam Java SDK provides a way to read and write text files using the `TextIO` class. Here is an example of reading a local text file named "**myfile.txt**" and printing its contents:

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

To write data to a local file, you can use the write method and pass in the file path as a string. Here is an example of writing a string to a local text file named "myfile.txt":

```
Pipeline p = Pipeline.create();
p.apply(Create.of("Hello, World!"))
 .apply(TextIO.write().to("myfile.txt"));
p.run();
```

It is important to note that the from and to methods only read and write to local file systems and not the distributed file systems like `HDFS`, `GCS`, `S3` etc.