---
layout: section
title: "Testing I/O Transforms"
section_menu: section-menu/documentation.html
permalink: /documentation/io/testing/
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

[Pipeline I/O Table of Contents]({{site.baseurl}}/documentation/io/io-toc/)


## Testing I/O Transforms in Apache Beam

*Examples and design patterns for testing Apache Beam I/O transforms*

<nav class="language-switcher">
  <strong>Adapt for:</strong>
  <ul>
    <li data-type="language-java" class="active">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>

> Note: This guide is still in progress. There is an open issue to finish the guide: [BEAM-1025](https://issues.apache.org/jira/browse/BEAM-1025).

## Introduction {#introduction}

This document explains the set of tests that the Beam community recommends based on our past experience writing I/O transforms. If you wish to contribute your I/O transform to the Beam community, we'll ask you to implement these tests.

While it is standard to write unit tests and integration tests, there are many possible definitions. Our definitions are:

*   **Unit Tests:**
    *   Goal: verifying correctness of the transform only - core behavior, corner cases, etc.
    *   Data store used: an in-memory version of the data store (if available), otherwise you'll need to write a [fake](#use-fakes)
    *   Data set size: tiny (10s to 100s of rows)
*   **Integration Tests:**
    *   Goal: catch problems that occur when interacting with real versions of the runners/data store
    *   Data store used: an actual instance, pre-configured before the test
    *   Data set size: small to medium (1000 rows to 10s of GBs)


## A note on performance benchmarking

We do not advocate writing a separate test specifically for performance benchmarking. Instead, we recommend setting up integration tests that can accept the necessary parameters to cover many different testing scenarios.

For example, if integration tests are written according to the guidelines below, the integration tests can be run on different runners (either local or in a cluster configuration) and against a data store that is a small instance with a small data set, or a large production-ready cluster with larger data set. This can provide coverage for a variety of scenarios - one of them is performance benchmarking.


## Test Balance - Unit vs Integration {#test-balance-unit-vs-integration}

It's easy to cover a large amount of code with an integration test, but it is then hard to find a cause for test failures and the test is flakier.

However, there is a valuable set of bugs found by tests that exercise multiple workers reading/writing to data store instances that have multiple nodes (eg, read replicas, etc.).  Those scenarios are hard to find with unit tests and we find they commonly cause bugs in I/O transforms.

Our test strategy is a balance of those 2 contradictory needs. We recommend doing as much testing as possible in unit tests, and writing a single, small integration test that can be run in various configurations.


## Examples {#examples}

Java:
*  [BigtableIO](https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/src/test/java/org/apache/beam/sdk/io/gcp/bigtable/BigtableIOTest.java)'s testing implementation is considered the best example of current best practices for unit testing `Source`s
*  [JdbcIO](https://github.com/apache/beam/blob/master/sdks/java/io/jdbc) has the current best practice examples for writing integration tests.
* [ElasticsearchIO](https://github.com/apache/beam/blob/master/sdks/java/io/elasticsearch) demonstrates testing for bounded read/write
* [MqttIO](https://github.com/apache/beam/tree/master/sdks/java/io/mqtt) and [AmpqpIO](https://github.com/apache/beam/tree/master/sdks/java/io/amqp) demonstrate unbounded read/write

Python:
* [avroio_test](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/avroio_test.py) for examples of testing liquid sharding, `source_test_utils`, `assert_that` and `equal_to`


## Unit Tests {#unit-tests}


### Goals {#goals}

*   Validate the correctness of the code in your I/O transform.
*   Validate that the I/O transform works correctly when used in concert with reference implementations of the data store it connects with (where "reference implementation" means a fake or in-memory version).
*   Be able to run quickly and need only one machine, with a reasonably small memory/disk footprint and no non-local network access (preferably none at all). Aim for tests than run within several seconds - anything above 20 seconds should be discussed with the beam dev mailing list.
*   Validate that the I/O transform can handle network failures.


### Non-goals

*   Test problems in the external data store - this can lead to extremely complicated tests.  


### Implementing unit tests {#implementing-unit-tests}

A general guide to writing Unit Tests for all transforms can be found in the [PTransform Style Guide](https://beam.apache.org/contribute/ptransform-style-guide/#testing ). We have expanded on a few important points below.

If you are using the `Source` API, make sure to exhaustively unit-test your code. A minor implementation error can lead to data corruption or data loss (such as skipping or duplicating records) that can be hard for your users to detect. Also look into using <span class="language-java">`SourceTestUtils`</span><span class="language-py">`source_test_utils`</span> - it is a key piece of testing `Source` implementations.

If you are not using the `Source` API, you can use `TestPipeline` with <span class="language-java">`PAssert`</span><span class="language-py">`assert_that`</span> to help with your testing.

If you are implementing write, you can use `TestPipeline` to write test data and then read and verify it using a non-Beam client.


### Use fakes {#use-fakes}

Instead of using mocks in your unit tests (pre-programming exact responses to each call for each test), use fakes. The preferred way to use fakes for I/O transform testing is to use a pre-existing in-memory/embeddable version of the service you're testing, but if one does not exist consider implementing your own. Fakes have proven to be the right mix of "you can get the conditions for testing you need" and "you don't have to write a million exacting mock function calls".


### Network failure

To help with testing and separation of concerns, **code that interacts across a network should be handled in a separate class from your I/O transform**. The suggested design pattern is that your I/O transform throws exceptions once it determines that a read or write is no longer possible.

This allows the I/O transform's unit tests to act as if they have a perfect network connection, and they do not need to retry/otherwise handle network connection problems.


## Batching

If your I/O transform allows batching of reads/writes, you must force the batching to occur in your test. Having configurable batch size options on your I/O transform allows that to happen easily. These must be marked as test only.


## I/O Transform Integration Tests {#i-o-transform-integration-tests}

> We do not currently have examples of Python I/O integration tests or integration tests for unbounded or eventually consistent data stores. We would welcome contributions in these areas - please contact the Beam dev@ mailing list for more information.

### Goals  {#it-goals}

*   Allow end to end testing of interactions between data stores, I/O transforms, and runners, simulating real world conditions.
*   Allow both small scale and large scale testing.
*   Self contained: require the least possible initial setup or existing outside state, besides the existence of a data store that the test can modify.
*   Anyone can run the same set of I/O transform integration tests that Beam runs on its continuous integration servers.


### Integration tests, data stores, and Kubernetes {#integration-tests-data-stores-and-kubernetes}

In order to test I/O transforms in real world conditions, you must connect to a data store instance.

The Beam community hosts the data stores used for integration tests in Kubernetes. In order for an integration test to be run in Beam's continuous integration environment, it must have Kubernetes scripts that set up an instance of the data store.

However, when working locally, there is no requirement to use Kubernetes. All of the test infrastructure allows you to pass in connection info, so developers can use their preferred hosting infrastructure for local development.


### Running integration tests {#running-integration-tests}

The high level steps for running an integration test are:
1.  Set up the data store corresponding to the test being run.
1.  Run the test, passing it connection info from the just created data store.
1.  Clean up the data store.

Since setting up data stores and running the tests involves a number of steps, and we wish to time these tests when running performance benchmarks, we use PerfKit Benchmarker to manage the process end to end. With a single command, you can go from an empty Kubernetes cluster to a running integration test.

However, **PerfKit Benchmarker is not required for running integration tests**. Therefore, we have listed the steps for both using PerfKit Benchmarker, and manually running the tests below.


#### Using PerfKit Benchmarker {#using-perfkit-benchmarker}

Prerequisites:
1.  [Install PerfKit Benchmarker](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker)
1.  Have a running Kubernetes cluster you can connect to locally using kubectl. We recommend using Google Kubernetes Engine - it's proven working for all the use cases we tested.  

You won’t need to invoke PerfKit Benchmarker directly. Run `./gradlew performanceTest` task in project's root directory, passing kubernetes scripts of your choice (located in .test_infra/kubernetes directory). It will setup PerfKitBenchmarker for you.  

Example run with the [Direct](https://beam.apache.org/documentation/runners/direct/) runner:
```
./gradlew performanceTest -DpkbLocation="/Users/me/PerfKitBenchmarker/pkb.py" -DintegrationTestPipelineOptions='["--numberOfRecords=1000"]' -DitModule=sdks/java/io/jdbc/ -DintegrationTest=org.apache.beam.sdk.io.jdbc.JdbcIOIT -DkubernetesScripts="/Users/me/beam/.test-infra/kubernetes/postgres/postgres-service-for-local-dev.yml" -DbeamITOptions="/Users/me/beam/.test-infra/kubernetes/postgres/pkb-config-local.yml" -DintegrationTestRunner=direct
```


Example run with the [Google Cloud Dataflow](https://beam.apache.org/documentation/runners/dataflow/) runner:
```
./gradlew performanceTest -DpkbLocation="/Users/me/PerfKitBenchmarker/pkb.py" -DintegrationTestPipelineOptions='["--numberOfRecords=1000", "--project=GOOGLE_CLOUD_PROJECT", "--tempRoot=GOOGLE_STORAGE_BUCKET"]' -DitModule=sdks/java/io/jdbc/ -DintegrationTest=org.apache.beam.sdk.io.jdbc.JdbcIOIT -DkubernetesScripts="/Users/me/beam/.test-infra/kubernetes/postgres/postgres-service-for-local-dev.yml" -DbeamITOptions="/Users/me/beam/.test-infra/kubernetes/postgres/pkb-config-local.yml" -DintegrationTestRunner=dataflow
```

Example run with the HDFS filesystem and Cloud Dataflow runner:

```
./gradlew performanceTest -DpkbLocation="/Users/me/PerfKitBenchmarker/pkb.py" -DintegrationTestPipelineOptions='["--numberOfRecords=100000", "--project=GOOGLE_CLOUD_PROJECT", "--tempRoot=GOOGLE_STORAGE_BUCKET"]' -DitModule=sdks/java/io/file-based-io-tests/ -DintegrationTest=org.apache.beam.sdk.io.text.TextIOIT -DkubernetesScripts=".test-infra/kubernetes/hadoop/LargeITCluster/hdfs-multi-datanode-cluster.yml,.test-infra/kubernetes/hadoop/LargeITCluster/hdfs-multi-datanode-cluster-for-local-dev.yml" -DbeamITOptions=".test-infra/kubernetes/hadoop/LargeITCluster/pkb-config.yml" -DintegrationTestRunner=dataflow -DbeamExtraProperties='[filesystem=hdfs]'
```

NOTE: When using Direct runner along with HDFS cluster, please set `export HADOOP_USER_NAME=root` before runnning `performanceTest` task.

Parameter descriptions:


<table class="table">
  <thead>
    <tr>
     <td>
      <strong>Option</strong>
     </td>
     <td>
       <strong>Function</strong>
     </td>
    </tr>
  </thead>
  <tbody>
    <tr>
     <td>-DpkbLocation
     </td>
     <td>Path to PerfKit Benchmarker project.
     </td>
    </tr>
    <tr>
     <td>-DintegrationTestPipelineOptions
     </td>
     <td>Passes pipeline options directly to the test being run. Note that some pipeline options may be runner specific (like "--project" or "--tempRoot"). 
     </td>
    </tr>
    <tr>
     <td>-DitModule
     </td>
     <td>Specifies the project submodule of the I/O to test.
     </td>
    </tr>
    <tr>
     <td>-DintegrationTest
     </td>
     <td>Specifies the test to be run (fully qualified reference to class/test method).
     </td>
    </tr>
    <tr>
     <td>-DkubernetesScripts
     </td>
     <td>Paths to scripts with necessary kubernetes infrastructure.
     </td>
    </tr>
    <tr>
      <td>-DbeamITOptions
      </td>
      <td>Path to file with Benchmark configuration (static and dynamic pipeline options. See below for description).
      </td>
    </tr>
    <tr>
      <td>-DintegrationTestRunner
      </td>
      <td>Runner to be used for running the test. Currently possible options are: direct, dataflow.
      </td>
    </tr>
    <tr>
      <td>-DbeamExtraProperties
      </td>
      <td>Any other "extra properties" to be passed to Gradle, eg. "'[filesystem=hdfs]'". 
      </td>
    </tr>
  </tbody>
</table>

#### Without PerfKit Benchmarker {#without-perfkit-benchmarker}

If you're using Kubernetes scripts to host data stores, make sure you can connect to your cluster locally using kubectl. If you have your own data stores already setup, you just need to execute step 3 from below list.

1.  Set up the data store corresponding to the test you wish to run. You can find Kubernetes scripts for all currently supported data stores in [.test-infra/kubernetes](https://github.com/apache/beam/tree/master/.test-infra/kubernetes).
    1.  In some cases, there is a setup script (*.sh). In other cases, you can just run ``kubectl create -f [scriptname]`` to create the data store.
    1.  Convention dictates there will be:
        1.  A yml script for the data store itself, plus a `NodePort` service. The `NodePort` service opens a port to the data store for anyone who connects to the Kubernetes cluster's machines from within same subnetwork. Such scripts are typically useful when running the scripts on Minikube Kubernetes Engine.
        1.  A separate script, with LoadBalancer service. Such service will expose an _external ip_ for the datastore. Such scripts are needed when external access is required (eg. on Jenkins). 
    1.  Examples:
        1.  For JDBC, you can set up Postgres: `kubectl create -f .test-infra/kubernetes/postgres/postgres.yml`
        1.  For Elasticsearch, you can run the setup script: `bash .test-infra/kubernetes/elasticsearch/setup.sh`
1.  Determine the IP address of the service:
    1.  NodePort service: `kubectl get pods -l 'component=elasticsearch' -o jsonpath={.items[0].status.podIP}`
    1.  LoadBalancer service:` kubectl get svc elasticsearch-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}'`
1.  Run the test using `integrationTest` gradle task and the instructions in the test class (e.g. see the instructions in JdbcIOIT.java).
1.  Tell Kubernetes to delete the resources specified in the Kubernetes scripts:
    1.  JDBC: `kubectl delete -f .test-infra/kubernetes/postgres/postgres.yml`
    1.  Elasticsearch: `bash .test-infra/kubernetes/elasticsearch/teardown.sh`

##### integrationTest Task {#integration-test-task}

Since `performanceTest` task involved running PerfkitBenchmarker, we can't use it to run the tests manually. For such purposes a more "low-level" task called `integrationTest` was introduced.  


Example usage on Cloud Dataflow runner: 

```
./gradlew integrationTest -p sdks/java/io/hadoop-input-format -DintegrationTestPipelineOptions='["--project=GOOGLE_CLOUD_PROJECT", "--tempRoot=GOOGLE_STORAGE_BUCKET", "--numberOfRecords=1000", "--postgresPort=5432", "--postgresServerName=SERVER_NAME", "--postgresUsername=postgres", "--postgresPassword=PASSWORD", "--postgresDatabaseName=postgres", "--postgresSsl=false", "--runner=TestDataflowRunner"]' -DintegrationTestRunner=dataflow --tests=org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIOIT
```

Example usage on HDFS filesystem and Direct runner: 

NOTE: Below setup will only work when /etc/hosts file contains entries with hadoop namenode and hadoop datanodes external IPs. Please see explanation in: [Small Cluster config file](https://github.com/apache/beam/blob/master/.test-infra/kubernetes/hadoop/SmallITCluster/pkb-config.yml) and [Large Cluster config file](https://github.com/apache/beam/blob/master/.test-infra/kubernetes/hadoop/LargeITCluster/pkb-config.yml).

```
export HADOOP_USER_NAME=root 

./gradlew integrationTest -p sdks/java/io/file-based-io-tests -DintegrationTestPipelineOptions='["--numberOfRecords=1000", "--filenamePrefix=hdfs://HDFS_NAMENODE:9000/XMLIOIT", "--hdfsConfiguration=[{\"fs.defaultFS\":\"hdfs://HDFS_NAMENODE:9000\",\"dfs.replication\":1,\"dfs.client.use.datanode.hostname\":\"true\" }]" ]' -DintegrationTestRunner=direct -Dfilesystem=hdfs --tests org.apache.beam.sdk.io.xml.XmlIOIT
```

Parameter descriptions:


<table class="table">
  <thead>
    <tr>
     <td>
      <strong>Option</strong>
     </td>
     <td>
       <strong>Function</strong>
     </td>
    </tr>
  </thead>
  <tbody>
    <tr>
     <td>-p sdks/java/io/file-based-io-tests/
     </td>
     <td>Specifies the project submodule of the I/O to test.
     </td>
    </tr>
    <tr>
     <td>-DintegrationTestPipelineOptions
     </td>
     <td>Passes pipeline options directly to the test being run.
     </td>
    </tr>
    <tr>
     <td>-DintegrationTestRunner
     </td>
     <td>Runner to be used for running the test. Currently possible options are: direct, dataflow.
     </td>
    </tr>
    <tr>
     <td>-Dfilesystem
     </td>
     <td>(optional, where applicable) Filesystem to be used to run the test. Currently possible options are: gcs, hdfs, s3. If not provided, local filesystem will be used. 
     </td>
    </tr>
    <tr>
     <td>--tests
     </td>
     <td>Specifies the test to be run (fully qualified reference to class/test method). 
     </td>
    </tr>
  </tbody>
</table>

#### Running Integration Tests on Pull Requests {#running-on-pull-requests}

Thanks to [ghprb](https://github.com/janinko/ghprb) plugin it is possible to run Jenkins jobs when specific phrase is typed in a Github Pull Request's comment. Integration tests that have Jenkins job defined can be triggered this way. You can run integration tests using these phrases:

<table class="table">
  <thead>
    <tr>
     <td>
      <strong>Test</strong>
     </td>
     <td>
       <strong>Phrase</strong>
     </td>
    </tr>
  </thead>
  <tbody>
    <tr>
     <td>JdbcIOIT
     </td>
     <td>Run Java JdbcIO Performance Test
     </td>
    </tr>
    <tr>
     <td>MongoDBIOIT
     </td>
     <td>Run Java MongoDBIO Performance Test
     </td>
    </tr>
    <tr>
     <td>HadoopInputFormatIOIT
     </td>
     <td>Run Java HadoopInputFormatIO Performance Test
     </td>
    </tr>
    <tr>
     <td>TextIO - local filesystem
     </td>
     <td>Run Java TextIO Performance Test 
     </td>
    </tr>
    <tr>
     <td>TextIO - HDFS
     </td>
     <td>Run Java TextIO Performance Test HDFS 
     </td>
    </tr>
    <tr>
     <td>Compressed TextIO - local filesystem
     </td>
     <td>Run Java CompressedTextIO Performance Test 
     </td>
    </tr>
    <tr>
     <td>Compressed TextIO - HDFS
     </td>
     <td>Run Java CompressedTextIO Performance Test HDFS 
     </td>
    </tr>
    <tr>
     <td>AvroIO - local filesystem
     </td>
     <td>Run Java AvroIO Performance Test 
     </td>
    </tr>
    <tr>
     <td>AvroIO - HDFS
     </td>
     <td>Run Java AvroIO Performance Test HDFS 
     </td>
    </tr>
    <tr>
     <td>TFRecordIO - local filesystem
     </td>
     <td>Run Java TFRecordIO Performance Test 
     </td>
    </tr>
    <tr>
     <td>ParquetIO - local filesystem
     </td>
     <td>Run Java ParquetIO Performance Test 
     </td>
    </tr>
    <tr>
     <td>XmlIO - local filesystem
     </td>
     <td>Run Java XmlIO Performance Test 
     </td>
    </tr>
    <tr>
     <td>XmlIO - HDFS
     </td>
     <td>Run Java XmlIO Performance Test on HDFS
     </td>
    </tr>
  </tbody>
</table>

Every job definition can be found in [.test-infra/jenkins](https://github.com/apache/beam/tree/master/.test-infra/jenkins). 
If you modified/added new Jenkins job definitions in your Pull Request, run the seed job before running the integration test (comment: "Run seed job").

### Performance testing dashboard {#performance-testing-dashboard}

We measure the performance of IOITs by gathering test execution times from Jenkins jobs that run periodically. The consequent results are stored in a database (BigQuery), therefore we can display them in a form of plots. 

The dashboard gathering all the results is available here: [Performance Testing Dashboard](https://apache-beam-testing.appspot.com/explore?dashboard=5755685136498688)

### Implementing Integration Tests {#implementing-integration-tests}

There are three components necessary to implement an integration test:
*   **Test code**: the code that does the actual testing: interacting with the I/O transform, reading and writing data, and verifying the data.
*   **Kubernetes scripts**: a Kubernetes script that sets up the data store that will be used by the test code.
*   **Integrate with PerfKit Benchmarker**: this allows users to easily invoke PerfKit Benchmarker, creating the Kubernetes resources and running the test code.

These three pieces are discussed in detail below.

#### Test Code {#test-code}

These are the conventions used by integration testing code:
*   **Your test should use pipeline options to receive connection information.**
    *   For Java, there is a shared pipeline options object in the io/common directory. This means that if there are two tests for the same data store (e.g. for `Elasticsearch` and the `HadoopInputFormatIO` tests), those tests share the same pipeline options.
*   **Generate test data programmatically and parameterize the amount of data used for testing.**
    *   For Java, `CountingInput` + `TestRow` can be combined to generate deterministic test data at any scale.
*   **Use a write then read style for your tests.**
    *   In a single `Test`, run a pipeline to do a write using your I/O transform, then run another pipeline to do a read using your I/O transform.
    *   The only verification of the data should be the result from the read. Don't validate the data written to the database in any other way.
    *   Validate the actual contents of all rows in an efficient manner. An easy way to do this is by taking a hash of the rows and combining them. `HashingFn` can help make this simple, and `TestRow` has pre-computed hashes.
    *   For easy debugging, use `PAssert`'s `containsInAnyOrder` to validate the contents of a subset of all rows.
*   **Tests should assume they may be run multiple times and/or simultaneously on the same database instance.**
    *   Clean up test data: do this in an `@AfterClass` to ensure it runs.
    *   Use unique table names per run (timestamps are an easy way to do this) and per-method where appropriate.

An end to end example of these principles can be found in [JdbcIOIT](https://github.com/ssisk/beam/blob/jdbc-it-perf/sdks/java/io/jdbc/src/test/java/org/apache/beam/sdk/io/jdbc/JdbcIOIT.java).


#### Kubernetes scripts {#kubernetes-scripts}

As discussed in [Integration tests, data stores, and Kubernetes](#integration-tests-data-stores-and-kubernetes), to have your tests run on Beam's continuous integration server, you'll need to implement a Kubernetes script that creates an instance of your data store.

If you would like help with this or have other questions, contact the Beam dev@ mailing list and the community may be able to assist you.

Guidelines for creating a Beam data store Kubernetes script:

1.  **You should define two Kubernetes scripts.**
    *   This is the best known way to implement item #1.
    *   The first script will contain the main datastore instance script (`StatefulSet`) plus a `NodePort` service exposing the data store. This will be the script run by the Beam Jenkins continuous integration server.
    *   The second script will define an additional `LoadBalancer` service, used to expose an external IP address to the data store if the Kubernetes cluster is on another network. This file's name is usually suffixed with '-for-local-dev'.
1.  **You must ensure that pods are recreated after crashes.**
    *   If you use a `pod` directly, it will not be recreated if the pod crashes or something causes the cluster to move the container for your pod.
    *   In most cases, you'll want to use `StatefulSet` as it supports persistent disks that last between restarts, and having a stable network identifier associated with the pod using a particular persistent disk. `Deployment` and `ReplicaSet` are also possibly useful, but likely in fewer scenarios since they do not have those features.
1.  **You should create separate scripts for small and large instances of your data store.**
    *   This seems to be the best way to support having both a small and large data store available for integration testing, as discussed in [Small Scale and Large Scale Integration Tests](#small-scale-and-large-scale-integration-tests).
1.  **You must use a Docker image from a trusted source and pin the version of the Docker image.**
    *   You should prefer images in this order:
        1.  An image provided by the creator of the data source/sink (if they officially maintain it). For Apache projects, this would be the official Apache repository.
        1.  Official Docker images, because they have security fixes and guaranteed maintenance.
        1.  Non-official Docker images, or images from other providers that have good maintainers (e.g. [quay.io](http://quay.io/)).


#### Integrate with PerfKit Benchmarker {#integrate-with-perfkit-benchmarker}

To allow developers to easily invoke your I/O integration test, you should create a PerfKit Benchmarker benchmark configuration file for the data store. Each pipeline option needed by the integration test should have a configuration entry. This is to be passed to perfkit via "beamITOptions" option in "performanceTest" task (described above). The goal is that a checked in config has defaults such that other developers can run the test without changing the configuration.


#### Defining the benchmark configuration file {#defining-the-benchmark-configuration-file}

The benchmark configuration file is a yaml file that defines the set of pipeline options for a specific data store. Some of these pipeline options are **static** - they are known ahead of time, before the data store is created (e.g. username/password). Others options are **dynamic** - they are only known once the data store is created (or after we query the Kubernetes cluster for current status).

All known cases of dynamic pipeline options are for extracting the IP address that the test needs to connect to. For I/O integration tests, we must allow users to specify:



*   The type of the IP address to get (load balancer/node address)
*   The pipeline option to pass that IP address to
*   How to find the Kubernetes resource with that value (ie. what load balancer service name? what node selector?)

The style of dynamic pipeline options used here should support a variety of other types of values derived from Kubernetes, but we do not have specific examples.

The dynamic pipeline options are:


<table class="table">
  <thead>
    <tr>
     <td>
       <strong>Type name</strong>
     </td>
     <td>
       <strong>Meaning</strong>
     </td>
     <td>
       <strong>Selector field name</strong>
     </td>
     <td>
       <strong>Selector field value</strong>
     </td>
    </tr>
  </thead>
  <tbody>
    <tr>
     <td>NodePortIp
     </td>
     <td>We will be using the IP address of a k8s NodePort service, the value will be an IP address of a Pod
     </td>
     <td>podLabel
     </td>
     <td>A kubernetes label selector for a pod whose IP address can be used to connect to
     </td>
    </tr>
    <tr>
     <td>LoadBalancerIp
     </td>
     <td>We will be using the IP address of a k8s LoadBalancer, the value will be an IP address of the load balancer
     </td>
     <td>serviceName
     </td>
     <td>The name of the LoadBalancer kubernetes service.
     </td>
    </tr>
  </tbody>
</table>

#### Benchmark configuration files: full example configuration file {#benchmark-configuration-files-full-example-configuration-file}

A configuration file will look like this:
```
static_pipeline_options:
  -postgresUser: postgres
  -postgresPassword: postgres
dynamic_pipeline_options:
  - paramName: PostgresIp
    type: NodePortIp
    podLabel: app=postgres
```


and may contain the following elements:


<table class="table">
  <thead>
    <tr>
     <td><strong>Configuration element</strong>
     </td>
     <td><strong>Description and how to change when adding a new test</strong>
     </td>
    </tr>
  </thead>
  <tbody>
    <tr>
     <td>static_pipeline_options
     </td>
     <td>The set of preconfigured pipeline options.
     </td>
    </tr>
    <tr>
     <td>dynamic_pipeline_options
     </td>
     <td>The set of pipeline options that PerfKit Benchmarker will determine at runtime.
     </td>
    </tr>
    <tr>
     <td>dynamic_pipeline_options.name
     </td>
     <td>The name of the parameter to be passed to gradle's invocation of the I/O integration test.
     </td>
    </tr>
    <tr>
     <td>dynamic_pipeline_options.type
     </td>
     <td>The method of determining the value of the pipeline options.
     </td>
    </tr>
    <tr>
     <td>dynamic_pipeline_options - other attributes
     </td>
     <td>These vary depending on the type of the dynamic pipeline option - see the table of dynamic pipeline options for a description.
     </td>
    </tr>
  </tbody>
</table>



#### Customizing PerfKit Benchmarker behaviour {#customizing-perf-kit-benchmarker-behaviour}

In most cases, to run the _performanceTest_ task it is sufficient to pass the properties described above, which makes it easy to use. However, users can customize Perfkit Benchmarker's behavior even more by pasing some extra Gradle properties:


<table class="table">
  <thead>
    <tr>
     <td><strong>PerfKit Benchmarker Parameter</strong>
     </td>
     <td><strong>Corresponding Gradle property</strong>
     </td>
     <td><strong>Default value</strong>
     </td>
     <td><strong>Description</strong>
     </td>
    </tr>
  </thead>
  <tbody>
    <tr>
     <td>dpb_log_level
     </td>
     <td>-DlogLevel
     </td>
     <td>INFO
     </td>
     <td>Data Processing Backend's log level.
     </td>
    </tr>
    <tr>
     <td>gradle_binary
     </td>
     <td>-DgradleBinary
     </td>
     <td>./gradlew
     </td>
     <td>Path to gradle binary.
     </td>
    </tr>
    <tr>
     <td>official
     </td>
     <td>-Dofficial
     </td>
     <td>false
     </td>
     <td>If true, the benchmark results are marked as "official" and can be displayed on PerfKitExplorer dashboards.
     </td>
    </tr>
    <tr>
     <td>benchmarks
     </td>
     <td>-Dbenchmarks
     </td>
     <td>beam_integration_benchmark
     </td>
     <td>Defines the PerfKit Benchmarker benchmark to run. This is same for all I/O integration tests.
     </td>
    </tr>
    <tr>
     <td>beam_prebuilt
     </td>
     <td>-DbeamPrebuilt
     </td>
     <td>true
     </td>
     <td>If false, PerfKit Benchmarker runs the build task before running the tests.
     </td>
    </tr>
    <tr>
     <td>beam_sdk
     </td>
     <td>-DbeamSdk
     </td>
     <td>java
     </td>
     <td>Beam's sdk to be used by PerfKit Benchmarker.
     </td>
    </tr>
    <tr>
     <td>beam_timeout
     </td>
     <td>-DitTimeout
     </td>
     <td>1200
     </td>
     <td>Timeout (in seconds) after which PerfKit Benchmarker will stop executing the benchmark (and will fail).
     </td>
    </tr>
    <tr>
     <td>kubeconfig
     </td>
     <td>-Dkubeconfig
     </td>
     <td>~/.kube/config
     </td>
     <td>Path to kubernetes configuration file.
     </td>
    </tr>
    <tr>
     <td>kubectl
     </td>
     <td>-Dkubectl
     </td>
     <td>kubectl
     </td>
     <td>Path to kubernetes executable.
     </td>
    </tr>
    <tr>
     <td>beam_extra_properties
     </td>
     <td>-DbeamExtraProperties
     </td>
     <td>(empty string)
     </td>
     <td>Any additional properties to be appended to benchmark execution command.
     </td>
    </tr>
  </tbody>
</table>

#### Small Scale and Large Scale Integration Tests {#small-scale-and-large-scale-integration-tests}

Apache Beam expects that it can run integration tests in multiple configurations:
*   Small scale
    *   Execute on a single worker on the runner (it should be *possible* but is not required).
    *   The data store should be configured to use a single node.
    *   The dataset can be very small (1000 rows).
*   Large scale
    *   Execute on multiple workers on the runner.
    *   The datastore should be configured to use multiple nodes.
    *   The data set used in this case is larger (10s of GBs).

You can do this by:
1.  Creating two Kubernetes scripts: one for a small instance of the data store, and one for a large instance.
1.  Having your test take a pipeline option that decides whether to generate a small or large amount of test data (where small and large are sizes appropriate to your data store)

An example of this is [HadoopInputFormatIO](https://github.com/apache/beam/tree/master/sdks/java/io/hadoop-input-format)'s tests.

<!--
# Next steps

If you have a well tested I/O transform, why not contribute it to Apache Beam? Read all about it:

[Contributing I/O Transforms]({{site.baseurl }}/documentation/io/contributing/)
-->
