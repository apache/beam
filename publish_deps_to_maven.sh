./gradlew -Ppublishing {\
:model:job-management,\
:model:pipeline,\
:runners:core-construction-java,\
:runners:google-cloud-dataflow-java,\
:runners:java-fn-execution,\
:sdks:java:core,\
:sdks:java:expansion-service,\
:sdks:java:extensions:arrow,\
:sdks:java:extensions:google-cloud-platform-core,\
:sdks:java:extensions:protobuf,\
:sdks:java:io:google-cloud-platform,\
:sdks:java:io:kafka\
}:publishToMavenLocal