<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

## Gradle preparation

To run this example your `build.gradle` file should contain the following task to execute the pipeline:

```
task executeCdapHubspot (type:JavaExec) {
    mainClass = System.getProperty("mainClass")
    classpath = sourceSets.main.runtimeClasspath
    systemProperties System.getProperties()
    args System.getProperty("exec.args", "").split()
}
```

## Running the CdapHubspotToTxt pipeline example

Gradle 'executeCdapHubspot' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapHubspot -DmainClass=org.apache.beam.examples.complete.cdap.hubspot.CdapHubspotToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

`CdapHubspotToTxt` pipeline parameters:
- `authToken` - Hubspot Private Application Access token
- `objectType` - Hubspot objects to pull supported by CDAP [Hubspot Batch Source](https://github.com/akvelon/cdap-hubspot/blob/release-1.1.0-authorization/docs/Hubspot-batchsource.md)
- `outputTxtFilePathPrefix` - path to output folder with filename prefix. It will write a set of .txt files with names like {prefix}-###.

Please see CDAP [Hubspot Batch Source](https://github.com/akvelon/cdap-hubspot/blob/release-1.1.0-authorization/docs/Hubspot-batchsource.md) for more information.

To execute this pipeline, specify the parameters in the following format:

```bash
 --authToken=your-private-app-access-token \
 --referenceName=your-reference-name \
 --objectType=Contacts \
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix
```

## Running the CdapHubspotStreamingToTxt pipeline example

Gradle 'executeCdapHubspot' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapHubspot -DmainClass=org.apache.beam.examples.complete.cdap.hubspot.CdapHubspotStreamingToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

`CdapHubspotStreamingToTxt` pipeline parameters:
- `authToken` - Hubspot Private Application Access token
- `objectType` - Hubspot objects to pull supported by CDAP [Hubspot Streaming Source](https://github.com/akvelon/cdap-hubspot/blob/release-1.1.0-authorization/docs/Hubspot-streamingsource.md)
- `outputTxtFilePathPrefix` - path to output folder with filename prefix. It will write a set of .txt files with names like {prefix}-###.
- `pullFrequencySec` - delay in seconds between polling for new records updates. (Optional)
- `startOffset` - inclusive start offset from which the reading should be started. (Optional)

Please see CDAP [Hubspot Streaming Source](https://github.com/akvelon/cdap-hubspot/blob/release-1.1.0-authorization/docs/Hubspot-streamingsource.md) for more information.

To execute this pipeline, specify the parameters in the following format:

```bash
 --authToken=your-private-app-access-token \
 --referenceName=your-reference-name \
 --objectType=Contacts \
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix \
 --pullFrequencySec=100 \
 --startOffset=1000
```

## Running the TxtToCdapHubspot pipeline example

Gradle 'executeCdapHubspot' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapHubspot -DmainClass=org.apache.beam.examples.complete.cdap.hubspot.TxtToCdapHubspot \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

`TxtToCdapHubspot` pipeline parameters:
- `authToken` - Hubspot Private Application Access token
- `objectType` - Hubspot objects to pull supported by [Hubspot Batch Sink](https://github.com/akvelon/cdap-hubspot/blob/release-1.1.0-authorization/docs/Hubspot-batchsink.md)
- `inputTxtFilePath` - input .txt file path
- `locksDirPath` - locks directory path where locks will be stored. This parameter is needed for Hadoop External Synchronization (mechanism for acquiring locks related to the write job).

Please see CDAP [Hubspot Batch Sink](https://github.com/akvelon/cdap-hubspot/blob/release-1.1.0-authorization/docs/Hubspot-batchsink.md) for more information.

To execute this pipeline, specify the parameters in the following format:

```bash
 --authToken=your-private-app-access-token \
 --referenceName=your-reference-name \
 --objectType=your-object-type \
 --inputTxtFilePath=your-path-to-input-txt-file \
 --locksDirPath=your-path-to-locks-dir
```
