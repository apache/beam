Prompt: 
How to get, set, and create new coders for my Apache Beam pipeline?

Response: 
Every ['PCollection'](https://beam.apache.org/documentation/basics/#pcollection) in an Apache Beam pipeline requires a corresponding ['Coder’](https://beam.apache.org/documentation/programming-guide/#specifying-coders). In most cases, Beam SDKs can automatically infer a 'Coder' for a 'PCollection' based on its element type or the producing transform. However, in some instances, you may need to explicitly set a ‘Coder’ or create a custom 'Coder'.

In the Apache Beam SDKs for Python and Java, the 'Coder' type provides the necessary methods for encoding and decoding data. To get, set, or register a coder for a particular pipeline, you can access and modify the pipeline’s ‘CoderRegistry’ object.

The examples below demonstrate how to get, set, and create new ‘Coders’ in Apache Beam pipelines using the Python and Java SDKs.

**Python SDK:**

In the Python SDK, you can use the following methods:
* To retrieve the pipeline’s ‘CoderRegistry’ object - ‘coders.registry’.
* To get the default ‘Coder’ for a type - ‘CoderRegistry.get_coder’.
* To set a new ‘Coder’ for the target type - ‘CoderRegistry.register_coder’.

Here is an example illustrating how to set the default ‘Coder’ in the Python SDK:

```python
apache_beam.coders.registry.register_coder(int, BigEndianIntegerCoder)
```

This example sets a default ‘Coder’, specifically ‘BigEndianIntegerCoder’, for 'int' values in the pipeline.

For custom or complex nested data types, you can implement a custom coder for your pipeline. To create a new ‘Coder’, you need to define a class that inherits from ‘Coder’ and implement the required methods: 
* The ‘encode’ method takes input values and encodes them into byte strings.
* The ‘decode’ method decodes the encoded byte string into its corresponding object.
* The ‘is_deterministic’ method (optional) specifies whether this coder encodes values deterministically or not. A deterministic coder produces the same encoded representation of a given object every time, even if it is called on different workers at different moments. The method returns ‘True’ or ‘False’ based on your implementation.

Here’s an example of a custom ‘Coder’ implementation in the Python SDK:
https://towardsdatascience.com/data-pipelines-with-apache-beam-86cd8eb55fd8 

**Java SDK:**

In the Java SDK, you can use the following methods:
* To retrieve the pipeline’s ‘CoderRegistry’ object - ‘Pipeline.getCoderRegistry’. 
* To get the coder for an existing ‘PCollection’ - ‘getCoder’.
* To get the default ‘Coder’ for a type - ‘CoderRegistry.getCoder’.
* To set a new default ‘Coder’ for the target type - ‘CoderRegistry.registerCoder’.

Here is an example of how you can set the default ‘Coder’ in the Java SDK:

```java
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);

CoderRegistry cr = p.getCoderRegistry();
cr.registerCoder(Integer.class, BigEndianIntegerCoder.class);
```
In this example, you use the method ‘CoderRegistry.registerCoder’ to register ‘BigEndianIntegerCoder’ for the target 'integer' type.

For custom or complex nested data types, you can implement a custom coder for your pipeline. For this, the ‘Coder’ class exposes the following key methods: 
* The ‘encode’ method takes input values and encodes them into byte strings.
* The ‘decode’ method decodes the encoded byte string into its corresponding object.
* The ‘verifyDeterministic’ method (optional) specifies whether this coder produces deterministic encodings. A deterministic coder produces the same encoded representation of a given object every time, even if it is called on different workers at different moments. The method will return 'NonDeterministicException' if the coder is not deterministic.

Here’s an example of a custom ‘Coder’ implementation in the Java SDK: 
https://www.waitingforcode.com/apache-beam/coders-apache-beam/read 

For more details about working with coders, you can refer to the [Apache Beam documentation on data encoding and type safety](https://beam.apache.org/documentation/programming-guide/#data-encoding-and-type-safety). 
