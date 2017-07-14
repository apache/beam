---
layout: default
title: "Testing I/O Transforms"
permalink: /documentation/io/testing/
---

[Pipeline I/O Table of Contents]({{site.baseurl}}/documentation/io/io-toc/)

# Testing I/O Transforms

> Note: This guide is still in progress. There is an open issue to finish the guide: [BEAM-1025](https://issues.apache.org/jira/browse/BEAM-1025).


## Testing IO Transforms in Apache Beam 

*Examples and design patterns for testing Apache Beam I/O transforms*


## Introduction {#introduction}

This document explains the set of tests that the Beam community recommends based on our past experience writing I/O transforms. If you wish to contribute your I/O transform to the Beam community, we'll ask you to implement these tests.

While it is standard to write unit tests and integration tests, there are many possible definitions. Our definitions are:

*   **Unit Tests:**
    *   Goal: verifying correctness of the transform itself - core behavior, corner cases, etc.
    *   Data store used: an in-memory version of the data store (if available), otherwise you'll need to write a [fake](#setting-up-mocks-fakes)
    *   Data set size: tiny (10s to 100s of rows)
*   **Integration Tests:**
    *   Goal: catch problems that occur when interacting with real versions of the runners/data store
    *   Data store used: an actual instance, pre-configured before the test
    *   Data set size: small to medium (1000 rows to 10s of GBs)


## A note on performance benchmarking

Doing performance benchmarking is definitely useful and would provide value to the beam community. However, we do not advocate writing a separate performance test specifically for this purpose. Instead, we advocate setting up integration tests so that they be used with different runners and data set sizes. 

For example, if integration tests are written according to the guidelines below, the integration tests can be run on different runners (either local or in a cluster configuration) and against a data store that is a small instance with a small data set, or a large production-ready cluster with larger data set. This can provide coverage for a variety of scenarios - one of them is performance benchmarking.

See the Integration Testing section for more information.


## Test Balance - Unit vs Integration {#test-balance-unit-vs-integration}

It's easy to cover a large amount of code with an integration test, but it is then hard to find a cause for failures and the test is flakier. 

However, there is a valuable set of bugs found by tests that exercise multiple workers reading/writing to data store instances that have multiple nodes (eg, read replicas, etc.).  Those scenarios are hard to find with unit tests and we find they commonly cause bugs in I/O transforms.

Our test strategy is a balance of those 2 contradictory needs. We recommend doing as much testing as possible in unit tests, and writing a single, small integration test that can be run in various configurations.


## Examples {#examples}



*   `BigtableIO`'s testing implementation is considered the best example of current best practices for unit testing `Source`s. 
*   `DatastoreIO` best demonstrates usage of the Service interface design pattern.
*   `JdbcIO` has the current best practice examples for writing integration tests.


## Unit Tests {#unit-tests}


### Goals {#goals}



*   Validate the correctness of the code in your I/O transform.
*   Validate that the I/O transform works correctly when used in concert with reference implementations of the data store it connects with (where "reference implementation" means a fake or in-memory version).
*   Be able to run quickly (< 1 sec) and need only one machine, with a reasonably small memory/disk footprint and no non-local network access (preferably none at all).
*   Validate that the I/O transform can handle network failures. 


### Non-goals



*   Test problems in the external data store - this can lead to extremely complicated tests.  


### Implementing unit tests {#implementing-unit-tests}

A general guide to writing Unit Tests for all transforms can be found in the [PTransform Style Guide](https://beam.apache.org/contribute/ptransform-style-guide/#testing ). We have expanded on a few important points below.

If you are implementing a `Source`/`Reader` class, make sure to exhaustively unit-test your code. A minor implementation error can lead to data corruption or data loss (such as skipping or duplicating records) that can be hard for your users to detect. Also look into using SourceTestUtils - it is a key piece of test `Source` implementations.

If you are not using the `Source` API, you can use DoFnTester to help with your testing. Datastore's I/O transforms have some good examples of how to use it in testing I/O transforms.


### Use mocks/fakes

Instead of using mocks in your unit tests (pre-programming exact responses to each call for each test), use fakes (a lightweight implementation of the service that behaves the same way at a very small scale) or an in-memory version of the service you're testing. This has proven to be the right mix of "you can get the conditions for testing you need" and "you don't have to write a million exacting mock function calls".


### Network failure

To help with testing and separation of concerns, **code that interacts across a network should be handled in a separate class from your I/O transform**, and thus should be unit tested separately from the I/O transform itself. In many cases, necessary network retries should be encapsulated within a fake implementation. 

The suggested design pattern is that your I/O transform throws exceptions once it determines that a read is no longer possible. This allows the I/O transform's unit tests to act as if they have a perfect network connection, and they do not need to retry/otherwise handle network connection problems.


## Batching

If your I/O transform allows batching of reads/writes, you must force the batching to occur in your test. Having configurable batch size options on your I/O transform allows that to happen easily (potentially marked as test-only)


# Next steps

If you have a well tested I/O transform, why not contribute it to Apache Beam? Read all about it:

[Contributing I/O Transforms]({{site.baseurl }}/documentation/io/contributing/)

