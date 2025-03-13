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

The Go SDK is [fully released as of v2.33.0](/blog/go-sdk-release/).

The Go SDK the first SDK purely on the [Beam Portability Framework](/roadmap/portability/)
and can execute pipelines on portable runners, like Flink, Spark, Samza, and Google Cloud Dataflow.

Current roadmap:
* continue building up unbounded pipeline facing features, as described on the [Beam Dev Wiki](https://cwiki.apache.org/confluence/display/BEAM/Supporting+Streaming+in+the+Go+SDK).
* improve IO support via cross language transforms, and add scalable native transforms. [Go SDK Connector Roadmap](/roadmap/connectors-go-sdk/)

Otherwise, improving examples and documentation for devs and users alike is ongoing.
Contributions are welcome. Please contact the [dev list](mailto:dev@beam.apache.org?subject=%5BGo%20SDK%5D%20How%20can%20I%20help%3F)
for assistance in finding a place to help out.

 - Issues: [sdk-go](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Asdk-go)
 - Contact: Robert Burke (@lostluck) [Email](mailto:lostluck@apache.org?subject=%5BGo%20SDK%20Roadmap%5D) - Please also cc the dev@beam.apache.org list. I strongly prefer public discussion of Go SDK matters.
