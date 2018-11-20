---
layout: section
title: "Authoring I/O Transforms - Overview"
section_menu: section-menu/documentation.html
permalink: /documentation/io/authoring-overview/
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

# Authoring I/O Transforms - Overview

_A guide for users who need to connect to a data store that isn't supported by the [Built-in I/O Transforms]({{site.baseurl }}/documentation/io/built-in/)_


* TOC
{:toc}

## Introduction
This guide covers how to implement I/O transforms in the Beam model. Beam pipelines use these read and write transforms to import data for processing, and write data to a store.

Reading and writing data in Beam is a parallel task, and using `ParDo`s, `GroupByKey`s, etc... is usually sufficient. Rarely, you will need the more specialized `Source` and `Sink` classes for specific features. There are changes coming soon (`SplittableDoFn`, [BEAM-65](https://issues.apache.org/jira/browse/BEAM-65)) that will make `Source` unnecessary.

As you work on your I/O Transform, be aware that the Beam community is excited to help those building new I/O Transforms and that there are many examples and helper classes.


## Suggested steps for implementers
1. Check out this guide and come up with your design. If you'd like, you can email the [Beam dev mailing list]({{ site.baseurl }}/get-started/support) with any questions you might have. It's good to check there to see if anyone else is working on the same I/O Transform.
2. If you are planning to contribute your I/O transform to the Beam community, you'll be going through the normal Beam contribution life cycle - see the [Apache Beam Contribution Guide]({{ site.baseurl }}/contribute/contribution-guide/) for more details.
3. As you're working on your IO transform, see the [PTransform Style Guide]({{ site.baseurl }}/contribute/ptransform-style-guide/) for specific information about writing I/O Transforms.


## Read transforms
Read transforms take data from outside of the Beam pipeline and produce `PCollection`s of data.

For data stores or file types where the data can be read in parallel, you can think of the process as a mini-pipeline. This often consists of two steps:
1. Splitting the data into parts to be read in parallel
2. Reading from each of those parts

Each of those steps will be a `ParDo`, with a `GroupByKey` in between. The `GroupByKey` is an implementation detail, but for most runners it allows the runner to use different numbers of workers for:
* Determining how to split up the data to be read into chunks - this will likely occur on very few workers
* Reading - will likely benefit from more workers

The `GroupByKey` will also allow Dynamic Work Rebalancing to occur (on supported runners).

Here are some examples of read transform implementations that use the "reading as a mini-pipeline" model when data can be read in parallel:
* **Reading from a file glob** - For example reading all files in "~/data/**"
  * Get File Paths `ParDo`: As input, take in a file glob. Produce a `PCollection` of strings, each of which is a file path.
  * Reading `ParDo`: Given the `PCollection` of file paths, read each one, producing a `PCollection` of records.
* **Reading from a NoSQL Database** (eg Apache HBase) - these databases often allow reading from ranges in parallel.
  * Determine Key Ranges `ParDo`: As input, receive connection information for the database and the key range to read from. Produce a `PCollection` of key ranges that can be read in parallel efficiently.
  * Read Key Range `ParDo`: Given the `PCollection` of key ranges, read the key range, producing a `PCollection` of records.

For data stores or files where reading cannot occur in parallel, reading is a simple task that can be accomplished with a single `ParDo`+`GroupByKey`. For example:
* **Reading from a database query** - traditional SQL database queries often can only be read in sequence. The `ParDo` in this case would establish a connection to the database and read batches of records, producing a `PCollection` of those records.
* **Reading from a gzip file** - a gzip file has to be read in order, so it cannot be parallelized. The `ParDo` in this case would open the file and read in sequence, producing a `PCollection` of records from the file.


### When to implement using the `Source` API
The above discussion is in terms of `ParDo`s - this is because `Source`s have proven to be tricky to implement. At this point in time, the recommendation is to **use  `Source` only if `ParDo` doesn't meet your needs**. A class derived from `FileBasedSource` is often the best option when reading from files.

 If you're trying to decide on whether or not to use `Source`, feel free to email the [Beam dev mailing list]({{ site.baseurl }}/get-started/support) and we can discuss the specific pros and cons of your case.

In some cases implementing a `Source` may be necessary or result in better performance.
* `ParDo`s will not work for reading from unbounded sources - they do not support checkpointing and don't support mechanisms like de-duping that have proven useful for streaming data sources.
* `ParDo`s cannot provide hints to runners about their progress or the size of data they are reading -  without size estimation of the data or progress on your read, the runner doesn't have any way to guess how large your read will be, and thus if it attempts to dynamically allocate workers, it does not have any clues as to how many workers you may need for your pipeline.
* `ParDo`s do not support Dynamic Work Rebalancing - these are features used by some readers to improve the processing speed of jobs (but may not be possible with your data source).
* `ParDo`s do not receive 'desired_bundle_size' as a hint from runners when performing initial splitting.
`SplittableDoFn` ([BEAM-65](https://issues.apache.org/jira/browse/BEAM-65)) will mitigate many of these concerns.


## Write transforms
Write transforms are responsible for taking the contents of a `PCollection` and transferring that data outside of the Beam pipeline.

Write transforms can usually be implemented using a single `ParDo` that writes the records received to the data store.

TODO: this section needs further explanation.

### When to implement using the `Sink` API
You are strongly discouraged from using the `Sink` class unless you are creating a `FileBasedSink`. Most of the time, a simple `ParDo` is all that's necessary. If you think you have a case that is only possible using a `Sink`, please email the [Beam dev mailing list]({{ site.baseurl }}/get-started/support).

# Next steps

This guide is still in progress. There is an open issue to finish the guide: [BEAM-1025](https://issues.apache.org/jira/browse/BEAM-1025).

<!-- TODO: commented out until this content is ready.
For more details on actual implementation, continue with one of the the language specific guides:

* [Authoring I/O Transforms - Python]({{site.baseurl }}/documentation/io/authoring-python/)
* [Authoring I/O Transforms - Java]({{site.baseurl }}/documentation/io/authoring-java/)
-->
