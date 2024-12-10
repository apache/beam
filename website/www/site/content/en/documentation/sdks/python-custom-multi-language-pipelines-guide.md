---
type: languages
title: "Python custom multi-language pipelines guide"
---
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

# Python custom multi-language pipelines guide

Apache Beam's powerful model enables the development of scalable, resilient, and production-ready transforms, but the process often requires significant time and effort.

With SDKs available in multiple languages (Java, Python, Golang, YAML, etc.), creating and maintaining transforms for each language becomes a challenge, particularly for IOs. 
Developers must navigate different APIs, address unique quirks, and manage ongoing maintenance—such as updates, 
new features, and documentation—while ensuring consistent behavior across SDKs. 
This results in redundant work, as the same functionality is implemented repeatedly for each language 
(M x N effort, where M is the number of SDKs and N is the number of transforms).

To streamline this process, Beam’s portability framework enables the use of portable transforms that can be shared across languages. 
This reduces duplication, allowing developers to focus on maintaining only N transforms. 
Pipelines combining [portable transforms](#portable-transform) from other SDK(s) are known as 
[“multi-language” pipelines](../programming-guide.md#13-multi-language-pipelines-multi-language-pipelines).

The SchemaTransform framework represents the latest advancement in enhancing this multi-language capability.

The following jumps straight into the guide. If you need help with some of the terminology, check out the [appendix](#appendix) section below.

## Create a Java SchemaTransform

For better readability, use [**TypedSchemaTransformProvider**](https://beam.apache.org/releases/javadoc/current/index.html?org/apache/beam/sdk/schemas/transforms/TypedSchemaTransformProvider.html), a [SchemaTransformProvider](#schematransformprovider) parameterized on a custom configuration type `T`.
TypedSchemaTransformProvider will take care of converting the custom type definition to a Beam [Schema](../basics.md#schema), and converting an instance to a Beam Row. 

```java
TypedSchemaTransformProvider<T> extends SchemaTransformProvider {
  String identifier();

  SchemaTransform from(T configuration);
}
```

### Implement a configuration

First, set up a Beam Schema-compatible configuration. This will be used to construct the transform.
AutoValue types are encouraged for readability. Adding the appropriate `@DefaultSchema` annotation will help Beam do the conversions mentioned above.

```java
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract static class MyConfiguration {
  public static Builder builder() {
    return new AutoValue_MyConfiguration.Builder();
  }
  @SchemaFieldDescription("Description of what foo does...")
  public abstract String getFoo();

  @SchemaFieldDescription("Description of what bar does...")
  public abstract Integer getBar();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFoo(String foo);

    public abstract Builder setBar(Integer bar);

    public abstract MyConfiguration build();
  }
}
```

This configuration is surfaced to foreign SDKs. For example, when using this transform in Python, use the following format:

```python
with beam.Pipeline() as p:
  (p 
   | Create([...]) 
   | MySchemaTransform(foo="abc", bar=123)
```

When using this transform in YAML, use the following format:

```yaml
pipeline:
  transforms:
    - type: Create
      ...
    - type: MySchemaTransform
      config:
        foo: "abc"
        bar: 123
```

### Implement a TypedSchemaTransformProvider
Next, implement the `TypedSchemaTransformProvider`. The following two methods are required:

- `identifier`: Returns a unique identifier for this transform. The [Beam standard](../programming-guide.md#1314-defining-a-urn)
follows this structure: `<namespace>:<org>:<functionality>:<version>`.

- `from`: Builds the transform using a provided configuration.

An [expansion service](#expansion-service) uses these methods to find and build the transform. The `@AutoService(SchemaTransformProvider.class)` annotation is also required to ensure this provider is recognized by the expansion service.

```java
@AutoService(SchemaTransformProvider.class)
public class MyProvider extends TypedSchemaTransformProvider<MyConfiguration> {
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:my_transform:v1";
  }

  @Override
  protected SchemaTransform from(MyConfiguration configuration) {
    return new MySchemaTransform(configuration);
  }

  private static class MySchemaTransform extends SchemaTransform {
    private final MyConfiguration config;
    MySchemaTransform(MyConfiguration configuration) {
        this.config = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
        PCollection<Row> inputRows = input.get("input");
        PCollection<Row> outputRows = inputRows.apply(
                new MyJavaTransform(config.getFoo(), config.getBar()));

        return PCollectionRowTuple.of("output", outputRows);
    }
  }
}
```

#### Additional metadata (optional)
The following optional methods can help provide relevant metadata:
- `description`: Provide a human-readable description for the transform. Remote SDKs can use this text to generate documentation.
- `inputCollectionNames`: Provide PCollection tags that this transform expects to take in.
- `outputCollectionNames`: Provide PCollection tags this transform expects to produce.

```java
  @Override
  public String description() {
    return "This transform does this and that...";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Arrays.asList("input_1", "input_2");
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("output");
  }
```

## Build an expansion service that contains the transform 

Use an expansion service to make the transform available to foreign SDKs.

First, build a shaded JAR file that includes:
1. the transform,
2. the [**ExpansionService artifact**](https://central.sonatype.com/artifact/org.apache.beam/beam-sdks-java-expansion-service),
3. and some additional dependencies.

### Gradle build file
```groovy
plugins {
    id 'com.github.johnrengelman.shadow' version '8.1.1'
    id 'application'
}

mainClassName = "org.apache.beam.sdk.expansion.service.ExpansionService"

dependencies {
    // Dependencies for your transform
    ...

    // Beam's expansion service
    runtimeOnly "org.apache.beam:beam-sdks-java-expansion-service:$beamVersion"
    // AutoService annotation for our SchemaTransform provider 
    compileOnly "com.google.auto.service:auto-service-annotations:1.0.1"
    annotationProcessor "com.google.auto.service:auto-service:1.0.1"
    // AutoValue annotation for our configuration object
    annotationProcessor "com.google.auto.value:auto-value:1.9"
}
```

Next, run the shaded JAR file, and provide a port to host the service. A list of available SchemaTransformProviders will be displayed.

```shell
$ java -jar path/to/my-expansion-service.jar 12345

Starting expansion service at localhost:12345

Registered transforms:
        ...
Registered SchemaTransformProviders:
        beam:schematransform:org.apache.beam:my_transform:v1
```

The transform is discoverable at `localhost:12345`. Foreign SDKs can now discover and add it to their pipelines. 
The next section demonstrates how to do this with a Python pipeline.

## Use the SchemaTransform in a Python pipeline

The Python SDK’s [**ExternalTransformProvider**](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.external_transform_provider.html#apache_beam.transforms.external_transform_provider.ExternalTransformProvider)
can dynamically generate wrappers for portable transforms.

```python
from apache_beam.transforms.external_transform_provider import ExternalTransformProvider
```

### Connect to an expansion service
First, connect to an expansion service that contains the transform. This section demonstrates two methods of connecting to the expansion service.

#### Connect to an already running service

If your expansion service JAR file is already running, pass in the address:

```python
provider = ExternalTransformProvider("localhost:12345")
```

#### Start a service based on a Java JAR file

If the service lives in a JAR file but isn’t currently running, use Beam utilities to run the service in a subprocess:

```python
from apache_beam.transforms.external import JavaJarExpansionService

provider = ExternalTransformProvider(
    JavaJarExpansionService("path/to/my-expansion-service.jar"))
```

You can also provide a list of services:

```python
provider = ExternalTransformProvider([
    "localhost:12345",
    JavaJarExpansionService("path/to/my-expansion-service.jar"),
    JavaJarExpansionService("path/to/another-expansion-service.jar")])
```

When initialized, the `ExternalTransformProvider` connects to the expansion service(s), 
retrieves all portable transforms, and generates a Pythonic wrapper for each one.

### Retrieve and use the transform

Retrieve the transform using its unique identifier and use it in your multi-language pipeline:

```python
identifier = "beam:schematransform:org.apache.beam:my_transform:v1"
MyTransform = provider.get_urn(identifier)

with beam.Pipeline() as p:
  p | beam.Create(...) | MyTransform(foo="abc", bar=123)
```


### Inspect the transform's metadata
You can learn more about a portable transform’s configuration by inspecting its metadata:

```python
import inspect

inspect.getdoc(MyTransform)
# Output: "This transform does this and that..."

inspect.signature(MyTransform)
# Output: (foo: "str: Description of what foo does...", 
#	     bar: "int: Description of what bar does....")
```

This metadata is generated directly from the provider's implementation. 
The class documentation is generated from the [optional **description** method](#additional-metadata). 
The signature information is generated from the `@SchemaFieldDescription` annotations in the [configuration object](#implement-a-configuration).

## Appendix

### Portable transform

Also known as a [cross-language transform](../glossary.md#cross-language-transforms): a transform that is made available to other SDKs (i.e. other languages) via an expansion service.
Such a transform must offer a way to be constructed using language-agnostic parameter types. 

### Expansion Service

A container that can hold multiple portable transforms. During pipeline expansion, this service will
- Look up the transform in its internal registry
- Build the transform in its native language using the provided configuration
- Expand the transform – i.e. construct the transform’s sub-graph to be inserted in the pipeline
- Establish a gRPC communication channel with the runner to exchange data and signals during pipeline execution.

### SchemaTransform

A transform that takes and produces PCollections of Beam Rows with a predefined Schema, i.e.:

```java
SchemaTransform extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {}
```

### SchemaTransformProvider

Produces a SchemaTransform using a provided configuration. An expansion service uses this interface to identify and build the transform for foreign SDKs.

```java
SchemaTransformProvider {
  String identifier();
  
  SchemaTransform from(Row configuration);

  Schema configurationSchema();
}
```