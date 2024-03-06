Prompt:
What is a Transformer Service in Apache Beam?

Response:
The `Transform service` is Docker Compose service included into Apache Beam SDK versions 2.49.0 and later. You can use Transform service to upgrade or downgrade Beam SDK version of individual supported transforms in your pipeline without changing the Beam version of the pipeline. This feature is only supported for Beam Java SDK 2.53.0 and later.

To use this feature simply execute Java pipeline with additional pipeline options specifying the URNs of the transforms you want to upgrade and the Beam version you want to upgrade to:

```java
--transformsToOverride=beam:transform:org.apache.beam:bigquery_read:v1 --transformServiceBeamVersion=2.xy.z
```

In the above example, `--transformsToOverride` specifies the URN of the transform you want to upgrade or downgrade, and `--transformServiceBeamVersion` specifies the Beam version you want to upgrade to.

The framework will automatically download the specified version of Docker containers for the transforms and use them in the pipeline. You must have Docker installed on the machine that starts the service.

Currently the following transforms are supported:

* BigQuery read transform: `beam:transform:org.apache.beam:bigquery_read:v1`
* BigQuery write transform: `beam:transform:org.apache.beam:bigquery_write:v1`
* Kafka read transform: `beam:transform:org.apache.beam:kafka_read_with_metadata:v2`
* Kafka write transform: `beam:transform:org.apache.beam:kafka_write:v2`

Transform service implement Beam expansion API. This means you can use the Transform service to construct and execute multi-language pipelines. For example, you can build a Python pipeline that uses Java `KafkaIO` transform and execute in without installing Java locally.

Transform service can be started automatically by Apache Beam SDK or manually by users:

```java
java -jar beam-sdks-java-transform-service-app-<Beam version for the jar>.jar --port <port> --beam_version <Beam version for the transform service> --project_name <a unique ID for the transform service> --command up
```

Beam transform service includes a number of transforms implemented in the Apache Beam Java and Python SDKs:

* Java transforms: Google Cloud I/O connectors, the Kafka I/O connector, and the JDBC I/O connector
* Python transforms: all portable transforms implemented within the Apache Beam Python SDK, such as RunInference and DataFrame transforms.























