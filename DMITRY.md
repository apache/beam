To run pipeline with runner_v2 and modified Beam, do these steps:

- Build Beam modules used in the pipeline classpath and publish them to local
  Maven repository:
  ```shell
  $ ./publish_deps_to_maven.sh
  ```
  This step should add Beam modules with version equal to the `version` value in 
  the 'gradle.properties' file to the local Maven repository directory, e.g.
  ```shell
  $ ls ~/.m2/repository/org/apache/beam/beam-runners-core-java/
  2.35.0-SNAPSHOT/
  ```
  Remember this version, you'll need it in the pipeline 'build.gradle' file.

  - The modules in './publish_deps_to_maven.sh' are hand-picked. You can push
    all Beam modules to Maven with the
    `./gradlew -Ppublishing publishToMavenLocal` command, but it will take long
    time and may fail for some irrelevant tasks.
  - If you messed up with Maven depot and want to start from scratch, simply
    drop the '~/.m2' directory.

- Build Beam java SDK container and publish it to the Google container
  repository:
  ```shell
  $ ./gradlew ./gradlew :sdks:java:container:java11:pushToGcr
  ```
  Edit the GCR container name in the
  'sdks/java/container/java11/build/build.gradle' file. Once the step completes,
  you can use the container name as an http address and see that the container
  is actually there, e.g.
  http://gcr.io/google.com/dataflow-streaming/dmitryo-test/beam_java11_sdk

- In the pipeline 'gradle.build' file, add `mavenLocal()` repository and set
  `beamVersion` equal to the `version` value from the 'gradle.properties' file
  in the root beam directory.

- Run your pipeline with additional `--experiments=use_runner_v2` and
  `--sdkContainerImage=${YOUR_GCR_CONTAINER}` args.

  To be on a safe side, make sure Beam and pipeline use the same Java version
  (I was using Java 11).

  Note that Beam from the (local) Maven repository is used to build the pipeline
  and to start its launcher program locally. But when the pipeline deploys
  workers in cloud, they'll be using Beam jars packaged into container. This is
  why you need to push Beam to two places. Note that if you do changes deep
  inside Beam that do not affect APIs used by the pipeline, e.g. add extra
  logging in the KafkaIO module or modify constants there, it's usually enough
  to only push these changes to the container. Next time for the pipeline starts
  with the new container, the workers will pick up the changes. And vice versa,
  if you modify Kafka IO in the Beam project and only push them to Maven and do
  not update the container, workers in cloud won't have these changes.

- I branched my changes from the `release-2.35.0` tag, but they should be easy
  to transfer to other versions too, e.g. the master head.