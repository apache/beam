---
title:  "Apache Beam 2.21.0"
date:   2020-05-27 00:00:01 -0800
categories: 
  - blog
authors:
  - ibzib
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

We are happy to present the new 2.21.0 release of Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2210-2020-05-27) for this release.
For more information on changes in 2.21.0, check out the
[detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12347143).

## I/Os
* Python: Deprecated module `apache_beam.io.gcp.datastore.v1` has been removed
as the client it uses is out of date and does not support Python 3
([BEAM-9529](https://issues.apache.org/jira/browse/BEAM-9529)).
Please migrate your code to use
[apache_beam.io.gcp.datastore.**v1new**](https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.datastore.v1new.datastoreio.html).
See the updated
[datastore_wordcount](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/cookbook/datastore_wordcount.py)
for example usage.
* Python SDK: Added integration tests and updated batch write functionality for Google Cloud Spanner transform ([BEAM-8949](https://issues.apache.org/jira/browse/BEAM-8949)).

## New Features / Improvements
* Python SDK will now use Python 3 type annotations as pipeline type hints.
([#10717](https://github.com/apache/beam/pull/10717))

    If you suspect that this feature is causing your pipeline to fail, calling
    `apache_beam.typehints.disable_type_annotations()` before pipeline creation
    will disable is completely, and decorating specific functions (such as 
    `process()`) with `@apache_beam.typehints.no_annotations` will disable it
    for that function.

    More details can be found in 
    [Ensuring Python Type Safety](https://beam.apache.org/documentation/sdks/python-type-safety/)
    and the Python SDK Typing Changes
    [blog post](https://beam.apache.org/blog/python-typing/).

* Java SDK: Introducing the concept of options in Beam Schema’s. These options add extra 
context to fields and schemas. This replaces the current Beam metadata that is present 
in a FieldType only, options are available in fields and row schemas. Schema options are
fully typed and can contain complex rows. *Remark: Schema aware is still experimental.*
([BEAM-9035](https://issues.apache.org/jira/browse/BEAM-9035))
* Java SDK: The protobuf extension is fully schema aware and also includes protobuf option
conversion to beam schema options. *Remark: Schema aware is still experimental.*
([BEAM-9044](https://issues.apache.org/jira/browse/BEAM-9044))
* Added ability to write to BigQuery via Avro file loads (Python) ([BEAM-8841](https://issues.apache.org/jira/browse/BEAM-8841))

    By default, file loads will be done using JSON, but it is possible to
    specify the temp_file_format parameter to perform file exports with AVRO.
    AVRO-based file loads work by exporting Python types into Avro types, so
    to switch to Avro-based loads, you will need to change your data types
    from Json-compatible types (string-type dates and timestamp, long numeric
    values as strings) into Python native types that are written to Avro
    (Python's date, datetime types, decimal, etc). For more information
    see https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#avro_conversions.
* Added integration of Java SDK with Google Cloud AI VideoIntelligence service 
([BEAM-9147](https://issues.apache.org/jira/browse/BEAM-9147))
* Added integration of Java SDK with Google Cloud AI natural language processing API
([BEAM-9634](https://issues.apache.org/jira/browse/BEAM-9634))
* `docker-pull-licenses` tag was introduced. Licenses/notices of third party dependencies will be added to the docker images when `docker-pull-licenses` was set. 
  The files are added to `/opt/apache/beam/third_party_licenses/`. 
  By default, no licenses/notices are added to the docker images. ([BEAM-9136](https://issues.apache.org/jira/browse/BEAM-9136))

 
## Breaking Changes

* Dataflow runner now requires the `--region` option to be set, unless a default value is set in the environment ([BEAM-9199](https://issues.apache.org/jira/browse/BEAM-9199)). See [here](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints) for more details.
* HBaseIO.ReadAll now requires a PCollection of HBaseIO.Read objects instead of HBaseQuery objects ([BEAM-9279](https://issues.apache.org/jira/browse/BEAM-9279)).
* ProcessContext.updateWatermark has been removed in favor of using a WatermarkEstimator ([BEAM-9430](https://issues.apache.org/jira/browse/BEAM-9430)).
* Coder inference for PCollection of Row objects has been disabled ([BEAM-9569](https://issues.apache.org/jira/browse/BEAM-9569)).
* Go SDK docker images are no longer released until further notice.

## Deprecations
* Java SDK: Beam Schema FieldType.getMetadata is now deprecated and is replaced by the Beam
Schema Options, it will be removed in version `2.23.0`. ([BEAM-9704](https://issues.apache.org/jira/browse/BEAM-9704))
* The `--zone` option in the Dataflow runner is now deprecated. Please use `--worker_zone` instead. ([BEAM-9716](https://issues.apache.org/jira/browse/BEAM-9716))


## List of Contributors

According to git shortlog, the following people contributed to the 2.21.0 release. Thank you to all contributors!

Aaron Meihm, Adrian Eka, Ahmet Altay, AldairCoronel, Alex Van Boxel, Alexey Romanenko, Andrew Crites, Andrew Pilloud, Ankur Goenka, Badrul (Taki) Chowdhury, Bartok Jozsef, Boyuan Zhang, Brian Hulette, brucearctor, bumblebee-coming, Chad Dombrova, Chamikara Jayalath, Chie Hayashida, Chris Gorgolewski, Chuck Yang, Colm O hEigeartaigh, Curtis "Fjord" Hawthorne, Daniel Mills, Daniel Oliveira, David Yan, Elias Djurfeldt, Emiliano Capoccia, Etienne Chauchot, Fernando Diaz, Filipe Regadas, Gleb Kanterov, Hai Lu, Hannah Jiang, Harch Vardhan, Heejong Lee, Henry Suryawirawan, Hk-tang, Ismaël Mejía, Jacoby, Jan Lukavský, Jeroen Van Goey, jfarr, Jozef Vilcek, Kai Jiang, Kamil Wasilewski, Kenneth Knowles, KevinGG, Kyle Weaver, Kyoungha Min, Luke Cwik, Maximilian Michels, Michal Walenia, Ning Kang, Pablo Estrada, paul fisher, Piotr Szuberski, Reuven Lax, Robert Bradshaw, Robert Burke, Rose Nguyen, Rui Wang, Sam Rohde, Sam Whittle, Spoorti Kundargi, Steve Koonce, sunjincheng121, Ted Yun, Tesio, Thomas Weise, Tomo Suzuki, Udi Meiri, Valentyn Tymofieiev, Vasu Nori, Yichi Zhang, yoshiki.obata, Yueyang Qiu
