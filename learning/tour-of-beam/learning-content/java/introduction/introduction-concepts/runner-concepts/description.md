### Overview

Apache Beam provides a portable API layer for building sophisticated data-parallel processing `pipelines` that may be executed across a diversity of execution engines, or `runners`. The core concepts of this layer are based upon the Beam Model (formerly referred to as the Dataflow Model), and implemented to varying degrees in each Beam `runner`.

### Direct runner
The Direct Runner executes pipelines on your machine and is designed to validate that pipelines adhere to the Apache Beam model as closely as possible. Instead of focusing on efficient pipeline execution, the Direct Runner performs additional checks to ensure that users do not rely on semantics that are not guaranteed by the model. Some of these checks include:

* enforcing immutability of elements
* enforcing encodability of elements
* elements are processed in an arbitrary order at all points
* serialization of user functions (DoFn, CombineFn, etc.)

Using the Direct Runner for testing and development helps ensure that pipelines are robust across different Beam runners. In addition, debugging failed runs can be a non-trivial task when a pipeline executes on a remote cluster. Instead, it is often faster and simpler to perform local unit testing on your pipeline code. Unit testing your pipeline locally also allows you to use your preferred local debugging tools.

### Specify your dependency

When using Java, you must specify your dependency on the Direct Runner in your pom.xml.

```
<dependency>
   <groupId>org.apache.beam</groupId>
   <artifactId>beam-runners-direct-java</artifactId>
   <version>2.41.0</version>
   <scope>runtime</scope>
</dependency>
```

### Set runner

In java, you need to set runner to `args` when you start the program.

```
--runner=DirectRunner
```