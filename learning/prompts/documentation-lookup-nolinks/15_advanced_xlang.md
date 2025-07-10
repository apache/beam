Prompt:
What is a multi-language pipeline in Apache Beam?

Response:
Apache Beam lets you combine transforms written in any supported SDK language (currently, Java and Python) and use them in one multi-language pipeline. These pipelines can, for instance, extract data from a Python source, apply processing via a Java transform, and then deliver the results to a Python sink. Another example is leveraging the Apache Kafka connector and SQL transform from the Java SDK within Python pipelines.

To learn how to create a multi-language pipeline, refer to the multi-language pipelines quickstarts for Java and Python on the Apache Beam website.

Depending on the SDK language of the pipeline, you can use a high-level SDK wrapper class or a low-level transform class to access a cross-language transform. For more information, see the 'Using cross-language transforms' section in the Apache Beam documentation.

To develop a cross-language transform, you need to define a Uniform Resource Name (URN) for registering the transform with an expansion service. For more information, see the 'Defining a URN' section in the Beam Programming Guide.
