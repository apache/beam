Prompt:
What is a multi-language pipeline in Apache Beam?
Response:
Beam lets you combine transforms written in any supported SDK language (currently, [Java](https://beam.apache.org/documentation/programming-guide/#1311-creating-cross-language-java-transforms) and [Python](https://beam.apache.org/documentation/programming-guide/#1312-creating-cross-language-python-transforms)) and use them in one multi-language pipeline. For example, a pipeline that reads from a Python source, processes the data using a Java transform, and writes the data to a Python sink is a multi-language pipeline.

For example, the [Apache Kafka connector](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/kafka.py) and [SQL transform](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/sql.py) from the Java SDK can be used in Python pipelines.

See quickstart examples for [Java](https://beam.apache.org/documentation/sdks/java-multi-language-pipelines) and [Python](https://beam.apache.org/documentation/sdks/python-multi-language-pipelines) to learn how to create a multi-language pipeline.

Depending on the SDK language of the pipeline, you can use a high-level SDK-wrapper class, or a low-level transform class to access a cross-language transform. See [Using cross-language transforms](https://beam.apache.org/documentation/programming-guide/#use-x-lang-transforms) section of Apache Beam Documentation.

Developing a cross-language transform involves defining a Uniform Resourse Name(URN) for registering the transform with an expansion service. See [Defining a URN](https://beam.apache.org/documentation/programming-guide/#1314-defining-a-urn) for additional information. and examples.



