Prompt:
What is a multi-language pipeline in Apache Beam?

Response:
Apache Beam lets you combine transforms written in any supported SDK language (currently, [Java](https://beam.apache.org/documentation/programming-guide/#1311-creating-cross-language-java-transforms) and [Python](https://beam.apache.org/documentation/programming-guide/#1312-creating-cross-language-python-transforms)) and use them in one multi-language pipeline. For example, a multi-language pipeline might read from a Python source, process the data using a Java transform, and write the data to a Python sink.

For example, you can use the [Apache Kafka connector](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/kafka.py) and [SQL transform](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/sql.py) from the Java SDK in Python pipelines.

To learn how to create a multi-language pipeline, see the quickstart examples for [Java](https://beam.apache.org/documentation/sdks/java-multi-language-pipelines) and [Python](https://beam.apache.org/documentation/sdks/python-multi-language-pipelines).

Depending on the SDK language of the pipeline, you can use a high-level SDK wrapper class or a low-level transform class to access a cross-language transform. For more information, see [Using cross-language transforms](https://beam.apache.org/documentation/programming-guide/#use-x-lang-transforms).

To develop a cross-language transform, you need to define a Uniform Resource Name(URN) for registering the transform with an expansion service. For more information, see [Defining a URN](https://beam.apache.org/documentation/programming-guide/#1314-defining-a-urn).
