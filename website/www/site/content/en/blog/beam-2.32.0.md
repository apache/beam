---

title:  "Apache Beam 2.32.0"

date:   2021-08-25 00:00:01 -0800

categories:

  - blog
  - release
authors:

  - angoenka

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

We are happy to present the new 2.32.0 release of Apache Beam. This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2320-2021-08-11) for this release.

<!-- more -->

For more information on changes in 2.32.0, check out the [detailed release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12319527&version=12349992).


## Highlights
* The [Beam DataFrame
  API](/documentation/dsls/dataframes/overview/) is no
  longer experimental! We've spent the time since the [2.26.0 preview
  announcement](/blog/dataframe-api-preview-available/)
  implementing the most frequently used pandas operations
  ([BEAM-9547](https://issues.apache.org/jira/browse/BEAM-9547)), improving
  [documentation](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.html)
  and [error messages](https://issues.apache.org/jira/browse/BEAM-12028),
  adding
  [examples](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/dataframe),
  integrating DataFrames with [interactive
  Beam](https://beam.apache.org/releases/pydoc/current/apache_beam.runners.interactive.interactive_beam.html),
  and of course finding and fixing
  [bugs](https://issues.apache.org/jira/issues/?jql=project%3DBEAM%20AND%20issuetype%3DBug%20AND%20status%3DResolved%20AND%20component%3Ddsl-dataframe).
  Leaving experimental just means that we now have high confidence in the API
  and recommend its use for production workloads. We will continue to improve
  the API, guided by your
  [feedback](/community/contact-us/).


## I/Os

* Added ability to use JdbcIO.Write.withResults without statement and preparedStatementSetter. ([BEAM-12511](https://issues.apache.org/jira/browse/BEAM-12511))
- Added ability to register URI schemes to use the S3 protocol via FileIO. ([BEAM-12435](https://issues.apache.org/jira/browse/BEAM-12435)).
* Respect number of shards set in SnowflakeWrite batch mode. ([BEAM-12715](https://issues.apache.org/jira/browse/BEAM-12715))
* Java SDK: Update Google Cloud Healthcare IO connectors from using v1beta1 to using the GA version.

## New Features / Improvements

* Add support to convert Beam Schema to Avro Schema for JDBC LogicalTypes:
  `VARCHAR`, `NVARCHAR`, `LONGVARCHAR`, `LONGNVARCHAR`, `DATE`, `TIME`
  (Java)([BEAM-12385](https://issues.apache.org/jira/browse/BEAM-12385)).
* Reading from JDBC source by partitions (Java) ([BEAM-12456](https://issues.apache.org/jira/browse/BEAM-12456)).
* PubsubIO can now write to a dead-letter topic after a parsing error (Java)([BEAM-12474](https://issues.apache.org/jira/browse/BEAM-12474)).
* New append-only option for Elasticsearch sink (Java) [BEAM-12601](https://issues.apache.org/jira/browse/BEAM-12601)

## Breaking Changes

* ListShards (with DescribeStreamSummary) is used instead of DescribeStream to list shards in Kinesis streams. Due to this change, as mentioned in [AWS documentation](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html), for fine-grained IAM policies it is required to update them to allow calls to ListShards and DescribeStreamSummary APIs. For more information, see [Controlling Access to Amazon Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html) ([BEAM-12225](https://issues.apache.org/jira/browse/BEAM-12225)).

## Deprecations

* Python GBK will stop supporting unbounded PCollections that have global windowing and a default trigger in Beam 2.33. This can be overriden with `--allow_unsafe_triggers`. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).
* Python GBK will start requiring safe triggers or the `--allow_unsafe_triggers` flag starting with Beam 2.33. ([BEAM-9487](https://issues.apache.org/jira/browse/BEAM-9487)).

## Bugfixes

* Fixed race condition in RabbitMqIO causing duplicate acks (Java) ([BEAM-6516](https://issues.apache.org/jira/browse/BEAM-6516)))


## List of Contributors



According to git shortlog, the following people contributed to the 2.32.0 release. Thank you to all contributors!



Ahmet Altay, Ajo Thomas, Alex Amato, Alexey Romanenko, Alex Koay, allenpradeep, Anant Damle, Andrew Pilloud, Ankur Goenka, Ashwin Ramaswami, Benjamin Gonzalez, BenWhitehead, Blake Williams, Boyuan Zhang, Brian Hulette, Chamikara Jayalath, Daniel Oliveira, Daniel Thevessen, daria-malkova, David Cavazos, David Huntsperger, dennisylyung, Dennis Yung, dmkozh, egalpin, emily, Esun Kim, Gabriel Melo de Paula, Harch Vardhan, Heejong Lee, heidimhurst, hoshimura, Iñigo San Jose Visiers, Ismaël Mejía, Jack McCluskey, Jan Lukavský, Justin King, Kenneth Knowles, KevinGG, Ke Wu, kileys, Kyle Weaver, Luke Cwik, Maksym Skorupskyi, masahitojp, Matthew Ouyang, Matthias Baetens, Matt Rudary, MiguelAnzoWizeline, Miguel Hernandez, Nikita Petunin, Ning Ding, Ning Kang, odidev, Pablo Estrada, Pascal Gillet, rafal.ochyra, raphael.sanamyan, Reuven Lax, Robert Bradshaw, Robert Burke, roger-mike, Ryan McDowell, Sam Rohde, Sam Whittle, Siyuan Chen, Teng Qiu, Tianzi Cai, Tobias Hermann, Tomo Suzuki, tvalentyn, Tyson Hamilton, Udi Meiri, Valentyn Tymofieiev, Vitaly Terentyev, Yichi Zhang, Yifan Mai, yoshiki.obata, Yu Feng, YuqiHuai, yzhang559, Zachary Houfek, zhoufek
