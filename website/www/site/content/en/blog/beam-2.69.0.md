---
title:  "Apache Beam 2.69.0"
date:   2025-10-28 15:00:00 -0500
categories:
  - blog
  - release
authors:
  - vterentev
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

We are happy to present the new 2.69.0 release of Beam.
This release includes both improvements and new functionality.
See the [download page](/get-started/downloads/#2690-2025-10-28) for this release.

<!--more-->

For more information on changes in 2.69.0, check out the [detailed release notes](https://github.com/apache/beam/milestone/37?closed=1).

## Highlights

* (Python) Add YAML Editor and Visualization Panel ([#35772](https://github.com/apache/beam/issues/35772)).
* (Java) Java 25 Support ([#35627](https://github.com/apache/beam/issues/35627)).

### I/Os

* Upgraded Iceberg dependency to 1.10.0 ([#36123](https://github.com/apache/beam/issues/36123)).

### New Features / Improvements

* Enhance JAXBCoder with XMLInputFactory support (Java) ([#36446](https://github.com/apache/beam/issues/36446)).
* Python examples added for CloudSQL enrichment handler on [Beam website](https://beam.apache.org/documentation/transforms/python/elementwise/enrichment-cloudsql/) (Python) ([#35473](https://github.com/apache/beam/issues/36095)).
* Support for batch mode execution in WriteToPubSub transform added (Python) ([#35990](https://github.com/apache/beam/issues/35990)).
* Added official support for Python 3.13 ([#34869](https://github.com/apache/beam/issues/34869)).
* Added an optional output_schema verification to all YAML transforms ([#35952](https://github.com/apache/beam/issues/35952)).
* Support for encryption when using GroupByKey added, along with `--gbek` pipeline option to automatically replace all GroupByKey transforms (Java/Python) ([#36214](https://github.com/apache/beam/issues/36214)).

### Breaking Changes

* (Python) `dill` is no longer a required, default dependency for Apache Beam ([#21298](https://github.com/apache/beam/issues/21298)).
  - This change only affects pipelines that explicitly use the `pickle_library=dill` pipeline option.
  - While `dill==0.3.1.1` is still pre-installed on the official Beam SDK base images, it is no longer a direct dependency of the apache-beam Python package. This means it can be overridden by other dependencies in your environment.
  - If your pipeline uses `pickle_library=dill`, you must manually ensure `dill==0.3.1.1` is installed in both your submission and runtime environments.
    - Submission environment: Install the dill extra in your local environment `pip install apache-beam[gcpdill]`.
    - Runtime (worker) environment: Your action depends on how you manage your worker's environment.
      - If using default containers or custom containers with the official Beam base image e.g. `FROM apache/beam_python3.10_sdk:2.69.0`
        - Add `dill==0.3.1.1` to your worker's requirements file (e.g., requirements.txt)
        - Pass this file to your pipeline using the --requirements_file requirements.txt pipeline option (For more details see [managing Dataflow dependencies](https://cloud.google.com/dataflow/docs/guides/manage-dependencies#py-custom-containers)).
      - If custom containers with a non-Beam base image e.g. `FROM python:3.9-slim`
        - Install apache-beam with the dill extra in your docker file e.g. `RUN pip install --no-cache-dir apache-beam[gcp,dill]`
  - If there is a dill version mismatch between submission and runtime environments you might encounter unpickling errors like `Can't get attribute '_create_code' on <module 'dill._dill' from...`.
  - If dill is not installed in the runtime environment you will see the error `ImportError: Pipeline option pickle_library=dill is set, but dill is not installed...`
  - Report any issues you encounter when using `pickle_library=dill` to the GitHub issue ([#21298](https://github.com/apache/beam/issues/21298))
* (Python) Added a `pickle_library=dill_unsafe` pipeline option. This allows overriding `dill==0.3.1.1` using dill as the pickle_library. Use with extreme caution. Other versions of dill has not been tested with Apache Beam ([#21298](https://github.com/apache/beam/issues/21298)).
* (Python) The deterministic fallback coder for complex types like NamedTuple, Enum, and dataclasses now normalizes filepaths for better determinism guarantees. This affects streaming pipelines updating from 2.68 to 2.69 that utilize this fallback coder. If your pipeline is affected, you may see a warning like: "Using fallback deterministic coder for type X...". To update safely sepcify the pipeline option `--update_compatibility_version=2.68.0` ([#36345](https://github.com/apache/beam/pull/36345)).
* (Python) Fixed transform naming conflict when executing DataTransform on a dictionary of PColls ([#30445](https://github.com/apache/beam/issues/30445)).
  This may break update compatibility if you don't provide a `--transform_name_mapping`.
* Removed deprecated Hadoop versions (2.10.2 and 3.2.4) that are no longer supported for [Iceberg](https://github.com/apache/iceberg/issues/10940) from IcebergIO ([#36282](https://github.com/apache/beam/issues/36282)).
* (Go) Coder construction on SDK side is more faithful to the specs from runners without stripping length-prefix. This may break streaming pipeline update as the underlying coder could be changed ([#36387](https://github.com/apache/beam/issues/36387)).
* Minimum Go version for Beam Go updated to 1.25.2 ([#36461](https://github.com/apache/beam/issues/36461)).
* (Java) DoFn OutputReceiver now requires implementing a builder method as part of extended metadata support for elements ([#34902](https://github.com/apache/beam/issues/34902)).
* (Java) Removed ProcessContext outputWindowedValue introduced in 2.68 that allowed setting offset and record Id. Use OutputReceiver's builder to set those field ([#36523](https://github.com/apache/beam/pull/36523)).

### Bugfixes

* Fixed passing of pipeline options to x-lang transforms when called from the Java SDK (Java) ([#36443](https://github.com/apache/beam/issues/36443)).
* PulsarIO has now changed support status from incomplete to experimental. Both read and writes should now minimally
  function (un-partitioned topics, without schema support, timestamp ordered messages for read) (Java)
  ([#36141](https://github.com/apache/beam/issues/36141)).
* Fixed Spanner Change Stream reading stuck issue due to watermark of partition moving backwards ([#36470](https://github.com/apache/beam/issues/36470)).

## List of Contributors

According to git shortlog, the following people contributed to the 2.69.0 release. Thank you to all contributors!

Abdelrahman Ibrahim, Ahmed Abualsaud, Andrew Crites, Arun Pandian, Bryan Dang, Chamikara Jayalath, Charles Nguyen, Chenzo, Clay Johnson, Danny McCormick, David A, Derrick Williams, Enrique Calderon, Hai Joey Tran, Ian Liao, Ian Mburu, Jack McCluskey, Jiang Zhu, Joey Tran, Kenneth Knowles, Kyle Stanley, Maciej Szwaja, Minbo Bae, Mohamed Awnallah, Radek Stankiewicz, Radosław Stankiewicz, Razvan Culea, Reuven Lax, Sagnik Ghosh, Sam Whittle, Shunping Huang, Steven van Rossum, Talat UYARER, Tanu Sharma, Tarun Annapareddy, Tom Stepp, Valentyn Tymofieiev, Vitaly Terentyev, XQ Hu, Yi Hu, Yilei, claudevdm, flpablo, fozzie15, johnjcasey, lim1t, parveensania, yashu
