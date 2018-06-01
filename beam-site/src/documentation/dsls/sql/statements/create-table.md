---
layout: section
title: "Beam SQL: CREATE TABLE Statement"
section_menu: section-menu/sdks.html
permalink: /documentation/dsls/sql/statements/create-table/
---

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
