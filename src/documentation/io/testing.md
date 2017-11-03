---
layout: section
title: "Testing I/O Transforms"
section_menu: section-menu/documentation.html
permalink: /documentation/io/testing/
---

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
1.  Set up the data store corresponding to the test being run
1.  Run the test, passing it connection info from the just created data store
1.  Clean up the data store

Since setting up data stores and running the tests involves a number of steps, and we wish to time these tests when running performance benchmarks, we use PerfKit Benchmarker to manage the process end to end. With a single command, you can go from an empty Kubernetes cluster to a running integration test.

However, **PerfKit Benchmarker is not required for running integration tests**. Therefore, we have listed the steps for both using PerfKit Benchmarker, and manually running the tests below.


#### Using PerfKit Benchmarker {#using-perfkit-benchmarker}

Prerequisites:
1.  [Install PerfKit Benchmarker](https://github.com/GoogleCloudPlatform/PerfKitBenchmarker)
1.  Have a running Kubernetes cluster you can connect to locally using kubectl

You won't need to invoke PerfKit Benchmarker directly. Run mvn verify in the directory of the I/O module you'd like to test, with the parameter io-it-suite when running in jenkins CI or with a kubernetes cluster on the same network or io-it-suite-local when running on a local dev box accessing a kubernetes cluster on a remote network.

Example run with the direct runner:
```
mvn verify -Dio-it-suite-local -pl sdks/java/io/jdbc,sdks/java/io/jdbc -DpkbLocation="/Users/me/dev/PerfKitBenchmarker/pkb.py" -DforceDirectRunner -DintegrationTestPipelineOptions=["--myTestParam=val"]
```


Example run with the Cloud Dataflow runner:
```
mvn verify -Dio-it-suite -pl sdks/java/io/jdbc -DintegrationTestPipelineOptions=["--project=PROJECT","--gcpTempLocation=GSBUCKET"] -DintegrationTestRunner=dataflow -DpkbLocation="/Users/me/dev/PerfKitBenchmarker/pkb.py"
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
     <td>-Dio-it-suite
     </td>
     <td>Invokes the call to PerfKit Benchmarker when running in apache beam's jenkins instance or with a kubernetes cluster on the same network.
     </td>
    </tr>
    <tr>
     <td>-Dio-it-suite-local
     </td>
     <td>io-it-suite-local when running on a local dev box accessing a kubernetes cluster on a remote network. May not be supported for all I/O transforms.
     </td>
    </tr>
    <tr>
     <td>-pl sdks/java/io/jdbc
     </td>
     <td>Specifies the maven project of the I/O to test.
     </td>
    </tr>
    <tr>
     <td>-Dkubectl="path-to-kubectl" -Dkubeconfig="path-to-kubeconfig"
     </td>
     <td>Options for specifying non-standard kubectl configurations. Optional. Defaults to "kubectl" and "~/.kube/config".
     </td>
    </tr>
    <tr>
     <td>integrationTestPipelineOptions
     </td>
     <td>Passes pipeline options directly to the test being run.
     </td>
    </tr>
    <tr>
     <td>-DforceDirectRunner
     </td>
     <td>Runs the test with the direct runner.
     </td>
    </tr>
  </tbody>
</table>



#### Without PerfKit Benchmarker {#without-perfkit-benchmarker}

If you're using Kubernetes, make sure you can connect to your cluster locally using kubectl. Otherwise, skip to step 3 below.

1.  Set up the data store corresponding to the test you wish to run. You can find Kubernetes scripts for all currently supported data stores in [.test-infra/kubernetes](https://github.com/apache/beam/tree/master/.test-infra/kubernetes).
    1.  In some cases, there is a setup script (*.sh). In other cases, you can just run ``kubectl create -f [scriptname]`` to create the data store.
    1.  Convention dictates there will be:
        1.  A core yml script for the data store itself, plus a `NodePort` service. The `NodePort` service opens a port to the data store for anyone who connects to the Kubernetes cluster's machines.
        1.  A separate script, called for-local-dev, which sets up a LoadBalancer service.
    1.  Examples:
        1.  For JDBC, you can set up Postgres: `kubectl create -f .test-infra/kubernetes/postgres/postgres.yml`
        1.  For Elasticsearch, you can run the setup script: `bash .test-infra/kubernetes/elasticsearch/setup.sh`
1.  Determine the IP address of the service:
    1.  NodePort service: `kubectl get pods -l 'component=elasticsearch' -o jsonpath={.items[0].status.podIP}`
    1.  LoadBalancer service:` kubectl get svc elasticsearch-external -o jsonpath='{.status.loadBalancer.ingress[0].ip}'`
1.  Run the test using the instructions in the class (e.g. see the instructions in JdbcIOIT.java)
1.  Tell Kubernetes to delete the resources specified in the Kubernetes scripts:
    1.  JDBC: `kubectl delete -f .test-infra/kubernetes/postgres/postgres.yml`
    1.  Elasticsearch: `bash .test-infra/kubernetes/elasticsearch/teardown.sh`


### Implementing Integration Tests {#implementing-integration-tests}

There are three components necessary to implement an integration test:
*   **Test code**: the code that does the actual testing: interacting with the I/O transform, reading and writing data, and verifying the data.
*   **Kubernetes scripts**: a Kubernetes script that sets up the data store that will be used by the test code.
*   **Integrate with PerfKit Benchmarker using io-it-suite**: this allows users to easily invoke PerfKit Benchmarker, creating the Kubernetes resources and running the test code.

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
1.  **You must only provide access to the data store instance via a `NodePort` service.**
    *   This is a requirement for security, since it means that only the local network has access to the data store. This is particularly important since many data stores don't have security on by default, and even if they do, their passwords will be checked in to our public Github repo.
1.  **You should define two Kubernetes scripts.**
    *   This is the best known way to implement item #1.
    *   The first script will contain the main datastore instance script (`StatefulSet`) plus a `NodePort` service exposing the data store. This will be the script run by the Beam Jenkins continuous integration server.
    *   The second script will define a `LoadBalancer` service, used for local development if the Kubernetes cluster is on another network. This file's name is usually suffixed with '-for-local-dev'.
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

To allow developers to easily invoke your I/O integration test, you must perform these two steps. The follow sections describe each step in more detail.
1.  Create a PerfKit Benchmarker benchmark configuration file for the data store. Each pipeline option needed by the integration test should have a configuration entry.
1.  Modify the per-I/O Maven pom configuration so that PerfKit Benchmarker can be invoked from Maven.

The goal is that a checked in config has defaults such that other developers can run the test without changing the configuration.


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
     <td>The set of preconfigured mvn pipeline options.
     </td>
    </tr>
    <tr>
     <td>dynamic_pipeline_options
     </td>
     <td>The set of mvn pipeline options that PerfKit Benchmarker will determine at runtime.
     </td>
    </tr>
    <tr>
     <td>dynamic_pipeline_options.name
     </td>
     <td>The name of the parameter to be passed to mvn's invocation of the I/O integration test.
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



#### Per-I/O mvn pom configuration {#per-i-o-mvn-pom-configuration}

Each I/O is responsible for adding a section to its pom with a profile that invokes PerfKit Benchmarker with the proper parameters during the verify phase. Below are the set of PerfKit Benchmarker parameters and how to configure them.

The [JdbcIO pom](https://github.com/apache/beam/blob/master/sdks/java/io/jdbc/pom.xml) has an example of how to put these options together into a profile and invoke Python+PerfKit Benchmarker with them.


<table class="table">
  <thead>
    <tr>
     <td><strong>PerfKit Benchmarker Parameter</strong>
     </td>
     <td><strong>Description</strong>
     </td>
     <td><strong>Example value</strong>
     </td>
    </tr>
  </thead>
  <tbody>
    <tr>
     <td>benchmarks
     </td>
     <td>Defines the PerfKit Benchmarker benchmark to run. This is same for all I/O integration tests.
     </td>
     <td>beam_integration_benchmark
     </td>
    </tr>
    <tr>
     <td>beam_location
     </td>
     <td>The location where PerfKit Benchmarker can find the Beam repository.
     </td>
     <td>${beamRootProjectDir} - this is a variable you'll need to define for each maven pom. See example pom for an example.
     </td>
    </tr>
    <tr>
     <td>beam_prebuilt
     </td>
     <td>Whether or not to rebuild the Beam repository before invoking the I/O integration test command.
     </td>
     <td>true
     </td>
    </tr>
    <tr>
     <td>beam_sdk
     </td>
     <td>Whether PerfKit Benchmarker will run the Beam SDK for Java or Python.
     </td>
     <td>java
     </td>
    </tr>
    <tr>
     <td>beam_runner_profile
     </td>
     <td>Optional command line parameter used to override the runner, allowing us to use the direct runner.
     </td>
     <td>Always use the predefined variable instead of specifying this parameter ${pkbBeamRunnerProfile}
     </td>
    </tr>
    <tr>
     <td>beam_runner_option
     </td>
     <td>Optional command line parameter used to override the runner, allowing us to use the direct runner.
     </td>
     <td>Always use the predefined variable instead of specifying this parameter ${pkbBeamRunnerOption}
     </td>
    </tr>
    <tr>
     <td>beam_it_module
     </td>
     <td>The path to the pom that contains the test (needed for invoking the test with PerfKit Benchmarker).
     </td>
     <td>sdks/java/io/jdbc
     </td>
    </tr>
    <tr>
     <td>beam_it_class
     </td>
     <td>The test to run.
     </td>
     <td>org.apache.beam.sdk.io.jdbc.JdbcIOIT
     </td>
    </tr>
    <tr>
     <td>beam_it_options
     </td>
     <td>Pipeline options for the beam job - meant to be a way to pass pipeline options the user specifies on the commandline when invoking io-it-suite
     </td>
     <td>Always use ${integrationTestPipelineOptions}, which allows the user to pass in parameters.
     </td>
    </tr>
    <tr>
     <td>kubeconfig
     </td>
     <td>The standard PerfKit Benchmarker parameter `kubeconfig`, which specifies where the Kubernetes config file lives.
     </td>
     <td>Always use ${kubeconfig}
     </td>
    </tr>
    <tr>
     <td>kubectl
     </td>
     <td>The standard PerfKit Benchmarker parameter `kubectl`, which specifies where the kubectl binary lives.
     </td>
     <td>Always use ${kubectl}
     </td>
    </tr>
    <tr>
     <td>beam_kubernetes_scripts
     </td>
     <td>The Kubernetes script files to create and teardown via create/delete. Specify absolute path.
     </td>
     <td>${beamRootProjectDir}/.test-infra/kubernetes/postgres/pkb-config.yml
     </td>
    </tr>
  </tbody>
</table>


There is also a set of Maven properties which are useful when invoking PerfKit Benchmarker. These properties are configured in the I/O parent pom, and some are only available when the io-it-suite profile is active in Maven.


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

An example of this is [HadoopInputFormatIO](https://github.com/apache/beam/tree/master/sdks/java/io/hadoop/input-format)'s tests.

<!--
# Next steps

If you have a well tested I/O transform, why not contribute it to Apache Beam? Read all about it:

[Contributing I/O Transforms]({{site.baseurl }}/documentation/io/contributing/)
-->
