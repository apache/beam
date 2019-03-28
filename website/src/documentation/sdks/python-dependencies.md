---
layout: section
title: "Python SDK dependencies"
section_menu: section-menu/sdks.html
permalink: /documentation/sdks/python-dependencies/
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

# Beam SDK for Python dependencies

The Beam SDKs depend on common third-party components which then
import additional dependencies. Version collisions can result in unexpected
behavior in the service. If you are using any of these packages in your code, be
aware that some libraries are not forward-compatible and you may need to pin to
the listed versions that will be in scope during execution.

<details open><summary markdown="span">2.11.0</summary>

<p>Beam SDK for Python 2.11.0 has the following compile and
  runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>Package</th><th>Version</th></tr>
  <tr><td>avro-python3</td><td>&gt;=1.8.1,&lt;2.0.0; python_version &gt;= "3.0"</td></tr>
  <tr><td>avro</td><td>&gt;=1.8.1,&lt;2.0.0; python_version &lt; "3.0"</td></tr>
  <tr><td>crcmod</td><td>&gt;=1.7,&lt;2.0</td></tr>
  <tr><td>dill</td><td>&gt;=0.2.9,&lt;0.2.10</td></tr>
  <tr><td>fastavro</td><td>&gt;=0.21.4,&lt;0.22</td></tr>
  <tr><td>future</td><td>&gt;=0.16.0,&lt;1.0.0</td></tr>
  <tr><td>futures</td><td>&gt;=3.2.0,&lt;4.0.0; python_version &lt; "3.0"</td></tr>
  <tr><td>google-apitools</td><td>&gt;=0.5.26,&lt;0.5.27</td></tr>
  <tr><td>google-cloud-bigquery</td><td>&gt;=1.6.0,&lt;1.7.0</td></tr>
  <tr><td>google-cloud-bigtable</td><td>==0.31.1</td></tr>
  <tr><td>google-cloud-core</td><td>==0.28.1</td></tr>
  <tr><td>google-cloud-pubsub</td><td>==0.39.0</td></tr>
  <tr><td>googledatastore</td><td>&gt;=7.0.1,&lt;7.1; python_version &lt; "3.0"</td></tr>
  <tr><td>grpcio</td><td>&gt;=1.8,&lt;2</td></tr>
  <tr><td>hdfs</td><td>&gt;=2.1.0,&lt;3.0.0</td></tr>
  <tr><td>httplib2</td><td>&gt;=0.8,&lt;=0.11.3</td></tr>
  <tr><td>mock</td><td>&gt;=1.0.1,&lt;3.0.0</td></tr>
  <tr><td>oauth2client</td><td>&gt;=2.0.1,&lt;4</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>&gt;=0.90.0,&lt;=0.90.4</td></tr>
  <tr><td>protobuf</td><td>&gt;=3.5.0.post1,&lt;4</td></tr>
  <tr><td>pyarrow</td><td>&gt;=0.11.1,&lt;0.12.0; python_version &gt;= "3.0" or platform_system != "Windows"</td></tr>
  <tr><td>pydot</td><td>&gt;=1.2.0,&lt;1.3</td></tr>
  <tr><td>pytz</td><td>&gt;=2018.3</td></tr>
  <tr><td>pyvcf</td><td>&gt;=0.6.8,&lt;0.7.0; python_version &lt; "3.0"</td></tr>
  <tr><td>pyyaml</td><td>&gt;=3.12,&lt;4.0.0</td></tr>
  <tr><td>typing</td><td>&gt;=3.6.0,&lt;3.7.0; python_version &lt; "3.5.0"</td></tr>
</table>

</details>

&nbsp;

<details><summary markdown="span">2.10.0</summary>

<p>Beam SDK for Python 2.10.0 has the following compile and
  runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>Package</th><th>Version</th></tr>
  <tr><td>avro-python3</td><td>&gt;=1.8.1,&lt;2.0.0; python_version &gt;= "3.0"</td></tr>
  <tr><td>avro</td><td>&gt;=1.8.1,&lt;2.0.0; python_version &lt; "3.0"</td></tr>
  <tr><td>crcmod</td><td>&gt;=1.7,&lt;2.0</td></tr>
  <tr><td>dill</td><td>&gt;=0.2.9,&lt;0.2.10</td></tr>
  <tr><td>fastavro</td><td>&gt;=0.21.4,&lt;0.22</td></tr>
  <tr><td>future</td><td>&gt;=0.16.0,&lt;1.0.0</td></tr>
  <tr><td>futures</td><td>&gt;=3.1.1,&lt;4.0.0</td></tr>
  <tr><td>google-apitools</td><td>&gt;=0.5.23,&lt;=0.5.24</td></tr>
  <tr><td>google-cloud-bigquery</td><td>&gt;=1.6.0,&lt;1.7.0</td></tr>
  <tr><td>google-cloud-pubsub</td><td>==0.39.0</td></tr>
  <tr><td>googledatastore</td><td>&gt;=7.0.1,&lt;7.1; python_version &lt; "3.0"</td></tr>
  <tr><td>grpcio</td><td>&gt;=1.8,&lt;2</td></tr>
  <tr><td>hdfs</td><td>&gt;=2.1.0,&lt;3.0.0</td></tr>
  <tr><td>httplib2</td><td>&gt;=0.8,&lt;=0.11.3</td></tr>
  <tr><td>mock</td><td>&gt;=1.0.1,&lt;3.0.0</td></tr>
  <tr><td>oauth2client</td><td>&gt;=2.0.1,&lt;4</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>&gt;=0.90.0,&lt;=0.90.4</td></tr>
  <tr><td>protobuf</td><td>&gt;=3.5.0.post1,&lt;4</td></tr>
  <tr><td>pyarrow</td><td>&gt;=0.11.1,&lt;0.12.0; python_version &gt;= "3.0" or platform_system != "Windows"</td></tr>
  <tr><td>pydot</td><td>&gt;=1.2.0,&lt;1.3</td></tr>
  <tr><td>pytz</td><td>&gt;=2018.3</td></tr>
  <tr><td>pyvcf</td><td>&gt;=0.6.8,&lt;0.7.0</td></tr>
  <tr><td>pyyaml</td><td>&gt;=3.12,&lt;4.0.0</td></tr>
  <tr><td>typing</td><td>&gt;=3.6.0,&lt;3.7.0; python_version &lt; "3.5.0"</td></tr>
</table>

</details>

<details><summary markdown="span">2.9.0</summary>

<p>Beam SDK for Python 2.9.0 has the following compile and
  runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>Package</th><th>Version</th></tr>
  <tr><td>avro</td><td>&gt;=1.8.1, &lt;2.0.0</td></tr>
  <tr><td>crcmod</td><td>&gt;=1.7, &lt;2.0</td></tr>
  <tr><td>dill</td><td>&gt;=0.2.6, &lt;=0.2.8.2</td></tr>
  <tr><td>fastavro</td><td>&gt;=0.21.4, &lt;0.22</td></tr>
  <tr><td>future</td><td>&gt;=0.16.0, &lt;1.0.0</td></tr>
  <tr><td>futures</td><td>&gt;=3.1.1, &lt;4.0.0</td></tr>
  <tr><td>google-apitools</td><td>&gt;=0.5.23, &lt;0.5.24</td></tr>
  <tr><td>google-cloud-pubsub</td><td>==0.35.4</td></tr>
  <tr><td>google-cloud-bigquery</td><td>&gt;=1.6.0, &lt;1.7.0</td></tr>
  <tr><td>googledatastore</td><td>&gt;=7.0.1, &lt;7.1</td></tr>
  <tr><td>grpcio</td><td>&gt;=1.8, &lt;2</td></tr>
  <tr><td>hdfs</td><td>&gt;=2.1.0, &lt;3.0.0</td></tr>
  <tr><td>httplib2</td><td>&gt;=0.8, &lt;=0.11.3</td></tr>
  <tr><td>mock</td><td>&gt;=1.0.1, &lt;3.0.0</td></tr>
  <tr><td>nose</td><td>&gt;=1.3.7</td></tr>
  <tr><td>numpy</td><td>&gt;=1.14.3, &lt;2</td></tr>
  <tr><td>oauth2client</td><td>&gt;=2.0.1, &lt;4</td></tr>
  <tr><td>parameterized</td><td>&gt;=0.6.0, &lt;0.7.0</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>&gt;=0.90.0, &lt;0.90.4</td></tr>
  <tr><td>protobuf</td><td>&gt;=3.5.0.post1, &lt;4</td></tr>
  <tr><td>pydot</td><td>&gt;=1.2.0, &lt;1.3</td></tr>
  <tr><td>pyhamcrest</td><td>&gt;=1.9, &lt;2.0</td></tr>
  <tr><td>pytz</td><td>&gt;=2018.3, &lt;=2018.4</td></tr>
  <tr><td>pyyaml</td><td>&gt;=3.12, &lt;4.0.0</td></tr>
  <tr><td>pyvcf</td><td>&gt;=0.6.8, &lt;0.7.0</td></tr>
  <tr><td>typing</td><td>&gt;=3.6.0, &lt;3.7.0</td></tr>
</table>

</details>

<details><summary markdown="span">2.8.0</summary>

<p>Beam SDK for Python 2.8.0 has the following compile and
  runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>Package</th><th>Version</th></tr>
  <tr><td>avro</td><td>&gt;=1.8.1, &lt;2.0.0</td></tr>
  <tr><td>crcmod</td><td>&gt;=1.7, &lt;2.0</td></tr>
  <tr><td>dill</td><td>&gt;=0.2.6, &lt;=0.2.8.2</td></tr>
  <tr><td>fastavro</td><td>&gt;=0.21.4, &lt;0.22</td></tr>
  <tr><td>future</td><td>&gt;=0.16.0, &lt;1.0.0</td></tr>
  <tr><td>futures</td><td>&gt;=3.1.1, &lt;4.0.0</td></tr>
  <tr><td>google-apitools</td><td>&gt;=0.5.18, &lt;0.5.20</td></tr>
  <tr><td>google-cloud-pubsub</td><td>==0.26.0</td></tr>
  <tr><td>google-cloud-bigquery</td><td>==0.25.0</td></tr>
  <tr><td>googledatastore</td><td>==7.0.1</td></tr>
  <tr><td>grpcio</td><td>&gt;=1.8, &lt;2</td></tr>
  <tr><td>hdfs</td><td>&gt;=2.1.0, &lt;3.0.0</td></tr>
  <tr><td>httplib2</td><td>&gt;=0.8, &lt;=0.11.3</td></tr>
  <tr><td>mock</td><td>&gt;=1.0.1, &lt;3.0.0</td></tr>
  <tr><td>nose</td><td>&gt;=1.3.7</td></tr>
  <tr><td>numpy</td><td>&gt;=1.14.3, &lt;2</td></tr>
  <tr><td>oauth2client</td><td>&gt;=2.0.1, &lt;5</td></tr>
  <tr><td>parameterized</td><td>&gt;=0.6.0, &lt;0.7.0</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>&gt;=0.90.0, &lt;0.90.4</td></tr>
  <tr><td>proto-google-cloud-pubsub-v1</td><td>==0.15.4</td></tr>
  <tr><td>protobuf</td><td>&gt;=3.5.0.post1, &lt;4</td></tr>
  <tr><td>pydot</td><td>&gt;=1.2.0, &lt;1.3</td></tr>
  <tr><td>pyhamcrest</td><td>&gt;=1.9, &lt;2.0</td></tr>
  <tr><td>pytz</td><td>&gt;=2018.3, &lt;=2018.4</td></tr>
  <tr><td>pyyaml</td><td>&gt;=3.12, &lt;4.0.0</td></tr>
  <tr><td>pyvcf</td><td>&gt;=0.6.8, &lt;0.7.0</td></tr>
  <tr><td>typing</td><td>&gt;=3.6.0, &lt;3.7.0</td></tr>
</table>

</details>

<details><summary markdown="span">2.7.0</summary>

<p>Beam SDK for Python 2.7.0 has the following compile and
  runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>Package</th><th>Version</th></tr>
  <tr><td>avro</td><td>&gt;=1.8.1, &lt;2.0.0</td></tr>
  <tr><td>crcmod</td><td>&gt;=1.7, &lt;2.0</td></tr>
  <tr><td>dill</td><td>&gt;=0.2.6, &lt;=0.2.8.2</td></tr>
  <tr><td>fastavro</td><td>==0.19.7</td></tr>
  <tr><td>future</td><td>&gt;=0.16.0, &lt;1.0.0</td></tr>
  <tr><td>futures</td><td>&gt;=3.1.1, &lt;4.0.0</td></tr>
  <tr><td>google-apitools</td><td>&gt;=0.5.18, &lt;0.5.20</td></tr>
  <tr><td>google-cloud-pubsub</td><td>==0.26.0</td></tr>
  <tr><td>google-cloud-bigquery</td><td>==0.25.0</td></tr>
  <tr><td>googledatastore</td><td>==7.0.1</td></tr>
  <tr><td>grpcio</td><td>&gt;=1.8, &lt;2</td></tr>
  <tr><td>hdfs</td><td>&gt;=2.1.0, &lt;3.0.0</td></tr>
  <tr><td>httplib2</td><td>&gt;=0.8, &lt;=0.11.3</td></tr>
  <tr><td>mock</td><td>&gt;=1.0.1, &lt;3.0.0</td></tr>
  <tr><td>nose</td><td>&gt;=1.3.7</td></tr>
  <tr><td>numpy</td><td>&gt;=1.14.3, &lt;2</td></tr>
  <tr><td>oauth2client</td><td>&gt;=2.0.1, &lt;5</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>&gt;=0.90.0, &lt;0.90.4</td></tr>
  <tr><td>proto-google-cloud-pubsub-v1</td><td>==0.15.4</td></tr>
  <tr><td>protobuf</td><td>&gt;=3.5.0.post1, &lt;4</td></tr>
  <tr><td>pydot</td><td>&gt;=1.2.0, &lt;1.3</td></tr>
  <tr><td>pyhamcrest</td><td>&gt;=1.9, &lt;2.0</td></tr>
  <tr><td>pytz</td><td>&gt;=2018.3, &lt;=2018.4</td></tr>
  <tr><td>pyyaml</td><td>&gt;=3.12, &lt;4.0.0</td></tr>
  <tr><td>pyvcf</td><td>&gt;=0.6.8, &lt;0.7.0</td></tr>
  <tr><td>six</td><td>&gt;=1.9, &lt;1.12</td></tr>
  <tr><td>typing</td><td>&gt;=3.6.0, &lt;3.7.0</td></tr>
</table>

</details>

<details><summary markdown="span">2.6.0</summary>

<p>Beam SDK for Python 2.6.0 has the following compile and
  runtime dependencies.</p>

<table class="table-bordered table-striped">
  <tr><th>Package</th><th>Version</th></tr>
  <tr><td>avro</td><td>&gt;=1.8.1,&lt;2.0.0</td></tr>
  <tr><td>crcmod</td><td>&gt;=1.7,&lt;2.0</td></tr>
  <tr><td>dill</td><td>&gt;=0.2.6,&lt;=0.2.8.2</td></tr>
  <tr><td>future</td><td>&gt;=0.16.0,&lt;1.0.0</td></tr>
  <tr><td>futures</td><td>&gt;=3.1.1,&lt;4.0.0</td></tr>
  <tr><td>google-apitools</td><td>&gt;=0.5.18,&lt;=0.5.20</td></tr>
  <tr><td>google-cloud-bigquery</td><td>==0.25.0</td></tr>
  <tr><td>google-cloud-pubsub</td><td>==0.26.0</td></tr>
  <tr><td>googledatastore</td><td>==7.0.1</td></tr>
  <tr><td>grpcio</td><td>&gt;=1.8,&lt;2</td></tr>
  <tr><td>hdfs</td><td>&gt;=2.1.0,&lt;3.0.0</td></tr>
  <tr><td>httplib2</td><td>&gt;=0.8,&lt;=0.11.3</td></tr>
  <tr><td>mock</td><td>&gt;=1.0.1,&lt;3.0.0</td></tr>
  <tr><td>oauth2client</td><td>&gt;=2.0.1,&lt;5</td></tr>
  <tr><td>proto-google-cloud-datastore-v1</td><td>&gt;=0.90.0,&lt;=0.90.4</td></tr>
  <tr><td>proto-google-cloud-pubsub-v1</td><td>==0.15.4</td></tr>
  <tr><td>protobuf</td><td>&gt;=3.5.0.post1,&lt;4</td></tr>
  <tr><td>pydot</td><td>&gt;=1.2.0,&lt;1.3</td></tr>
  <tr><td>pytz</td><td>&gt;=2018.3,&lt;=2018.4</td></tr>
  <tr><td>pyvcf</td><td>&gt;=0.6.8,&lt;0.7.0</td></tr>
  <tr><td>pyyaml</td><td>&gt;=3.12,&lt;4.0.0</td></tr>
  <tr><td>six</td><td>&gt;=1.9,&lt;1.12</td></tr>
  <tr><td>typing</td><td>&gt;=3.6.0,&lt;3.7.0</td></tr>
</table>

</details>

