---
title: "Testing I/O Transforms"
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

## Testing I/O Transforms in Apache Beam

*Examples and design patterns for testing Apache Beam I/O transforms*

{{< language-switcher java py >}}

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

A general guide to writing Unit Tests for all transforms can be found in the [PTransform Style Guide](/contribute/ptransform-style-guide/#testing ). We have expanded on a few important points below.

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


### Running integration tests on your machine {#running-integration-tests-on-your-machine}

You can always run the IO integration tests on your own machine. The high level steps for running an integration test are:
1.  Set up the data store corresponding to the test being run.
1.  Run the test, passing it connection info from the just created data store.
1.  Clean up the data store.


#### Data store setup/cleanup {#datastore-setup-cleanup}

If you're using Kubernetes scripts to host data stores, make sure you can connect to your cluster locally using kubectl. If you have your own data stores already setup, you just need to execute step 3 from below list.

1.  Set up the data store corresponding to the test you wish to run. You can find Kubernetes scripts for all currently supported data stores in [.test-infra/kubernetes](https://github.com/apache/beam/tree/master/.test-infra/kubernetes).
    1.  In some cases, there is a dedicated setup script (*.sh). In other cases, you can just run ``kubectl create -f [scriptname]`` to create the data store. You can also let [kubernetes.sh](https://github.com/apache/beam/blob/master/.test-infra/kubernetes/kubernetes.sh) script perform some standard steps for you.
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

#### Running a particular test {#running-a-test}

`integrationTest` is a dedicated gradle task for running IO integration tests.

Example usage on Cloud Dataflow runner:

```
./gradlew integrationTest -p sdks/java/io/hadoop-format -DintegrationTestPipelineOptions='["--project=GOOGLE_CLOUD_PROJECT", "--tempRoot=GOOGLE_STORAGE_BUCKET", "--numberOfRecords=1000", "--postgresPort=5432", "--postgresServerName=SERVER_NAME", "--postgresUsername=postgres", "--postgresPassword=PASSWORD", "--postgresDatabaseName=postgres", "--postgresSsl=false", "--runner=TestDataflowRunner"]' -DintegrationTestRunner=dataflow --tests=org.apache.beam.sdk.io.hadoop.format.HadoopFormatIOIT
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

### Running Integration Tests on Pull Requests {#running-integration-tests-on-pull-requests}

Most of the IO integration tests have dedicated Jenkins jobs that run periodically to collect metrics and avoid regressions. Thanks to [ghprb](https://github.com/janinko/ghprb) plugin it is also possible to trigger these jobs on demand once a specific phrase is typed in a Github Pull Request's comment. This way tou can check if your contribution to a certain IO is an improvement or if it makes things worse (hopefully not!).

To run IO Integration Tests type the following comments in your Pull Request:

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
     <td>HadoopFormatIOIT
     </td>
     <td>Run Java HadoopFormatIO Performance Test
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

As mentioned before, we measure the performance of IOITs by gathering test execution times from Jenkins jobs that run periodically. The consequent results are stored in a database (BigQuery), therefore we can display them in a form of plots.

The dashboard gathering all the results is available here: [Performance Testing Dashboard](https://s.apache.org/io-test-dashboards)

### Implementing Integration Tests {#implementing-integration-tests}

There are three components necessary to implement an integration test:
*   **Test code**: the code that does the actual testing: interacting with the I/O transform, reading and writing data, and verifying the data.
*   **Kubernetes scripts**: a Kubernetes script that sets up the data store that will be used by the test code.
*   **Jenkins jobs**: a Jenkins Job DSL script that performs all necessary steps for setting up the data sources, running and cleaning up after the test.

These two pieces are discussed in detail below.

#### Test Code {#test-code}

These are the conventions used by integration testing code:
*   **Your test should use pipeline options to receive connection information.**
    *   For Java, there is a shared pipeline options object in the io/common directory. This means that if there are two tests for the same data store (e.g. for `Elasticsearch` and the `HadoopFormatIO` tests), those tests share the same pipeline options.
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
        1.  Non-official Docker images, or images from other providers that have good maintainers (e.g. [quay.io](https://quay.io/)).

#### Jenkins jobs {#jenkins-jobs}

You can find examples of existing IOIT jenkins job definitions in [.test-infra/jenkins](https://github.com/apache/beam/tree/master/.test-infra/jenkins) directory. Look for files caled job_PerformanceTest_*.groovy. The most prominent examples are:
* [JDBC](https://github.com/apache/beam/blob/master/.test-infra/jenkins/job_PerformanceTests_JDBC.groovy) IOIT job
* [MongoDB](https://github.com/apache/beam/blob/master/.test-infra/jenkins/job_PerformanceTests_MongoDBIO_IT.groovy) IOIT job
* [File-based](https://github.com/apache/beam/blob/master/.test-infra/jenkins/job_PerformanceTests_FileBasedIO_IT.groovy) IOIT jobs

Notice that there is a utility class helpful in creating the jobs easily without forgetting important steps or repeating code. See [Kubernetes.groovy](https://github.com/apache/beam/blob/master/.test-infra/jenkins/Kubernetes.groovy) for more details.

### Small Scale and Large Scale Integration Tests {#small-scale-and-large-scale-integration-tests}

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

An example of this is [HadoopFormatIO](https://github.com/apache/beam/tree/master/sdks/java/io/hadoop-format)'s tests.

<!--
# Next steps

If you have a well tested I/O transform, why not contribute it to Apache Beam? Read all about it:

[Contributing I/O Transforms](/documentation/io/contributing/)
-->
