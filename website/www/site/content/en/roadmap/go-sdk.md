---
title: "Go SDK Roadmap"
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

# Go SDK Roadmap

The Go SDK is currently experimental. As the first purely portable Beam SDK, the Go SDK is constrained
by the status of the [Beam Portability Framework](https://beam.apache.org/roadmap/portability/) and the existence of
portable runners.

**April 2020 Update**
This year we hope to move the SDK out of experimental at least for Batch usage. 

To do so, there are a few blocking changes:
  * Support Interoperability with other SDKs
  * Scalable IOs
  * Portability Proto stability
  * Go Ecosystem integration improvements

Interoperability while not a blocking feature in itself, requires Beam Schema support, which
the SDK could then use as it's default coder. Batch Splittable DoFns are nearly ready for
playing with on Flink, and the Python Direct Runner, answering the scalable batch IO question.
There's much work getting the portability protos to a stable baseline. This will allow runner
and SDK independance, so they don't need to be updated in lockstep. Finally, the Go SDK should
adopt Go Modules as it's versioning solution, and officially "catch up" with the current beam
version.

The Go SDK has the following goals for the next few months:

## Usability

* Beam Schema Support [BEAM-9615](https://issues.apache.org/jira/browse/BEAM-4124)
* Improvements to starcgen [BEAM-9616](https://issues.apache.org/jira/browse/BEAM-9616)

## Integrating with the Go ecosystem

The authors of the Go SDK intended to keep the parts of the language we love, and intend for
user pipelines to be as close to ordinary Go as possible, minimizing framework boiler plate code.

* Migrate to a vanity URL path [BEAM-4115](https://issues.apache.org/jira/browse/BEAM-4115)
* Package versioning support [BEAM-5379](https://issues.apache.org/jira/browse/BEAM-5379)

## Implement Portability features

The following are dependant on the features being implemented in the Portability Framework.

* Splitable DoFns (SDFs) for Scalable IO [BEAM-3301](https://issues.apache.org/jira/browse/BEAM-3301)
  * This is the primary blocker for the Go SDK to scale to ingest large numbers of elements in both batch and streaming.
* Triggers and Timers [BEAM-3304](https://issues.apache.org/jira/browse/BEAM-3304)
* Advanced WindowFns
   * Session windows [BEAM-4152](https://issues.apache.org/jira/browse/BEAM-4152)
   * Custom WindowFns (dependant on streaming Splitable DoFn support) [BEAM-2939](https://issues.apache.org/jira/browse/BEAM-2939)

Without SDFs, IOs are always constrained to work on a single machine prior to a sharding operation (like CoGroupByKey),
which makes scalable IOs difficult to impossible to write.

Otherwise, improving examples and documentation for devs and users alike is ongoing. 
Contributions are welcome. Please contact the [dev list](mailto:dev@beam.apache.org?subject=%5BGo%20SDK%5D%20How%20can%20I%20help%3F)
for assistance in finding a place to help out.

 - JIRA: [sdk-go](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20sdk-go)
 - Contact: Robert Burke (@lostluck) [Email](mailto:lostluck@apache.org?subject=%5BGo%20SDK%20Roadmap%5D) - Please also cc the dev@beam.apache.org list. I strongly prefer public discussion of Go SDK matters. 
