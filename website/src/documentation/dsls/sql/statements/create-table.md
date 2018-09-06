---
layout: section
title: "Beam SQL: CREATE TABLE Statement"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/statements/create-table/
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

# CREATE TABLE

Beam is not a storage system but reads and writes from other storage systems.
You register those systems with a `CREATE TABLE` statement that includes a schema
as well as a number of extended clauses:

 - `TYPE` to indicate what
 - `LOCATION` to specify a URL or otherwise indicate where the data is
 - `TBLPROPERTIES` to configure the endpoint

Once a table is registered, it may be read-only or it may support both read and
write access.

Currently there are a few experimental connectors available, and the reference
for them is their Javadoc:

 - [Kafka]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/meta/provider/kafka/KafkaTableProvider.html)
 - [Text]({{ site.baseurl }}/documentation/sdks/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/extensions/sql/meta/provider/text/TextTableProvider.html)
