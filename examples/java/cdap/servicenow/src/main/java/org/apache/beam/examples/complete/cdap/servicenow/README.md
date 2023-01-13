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
task executeCdapServiceNow (type:JavaExec) {
    mainClass = System.getProperty("mainClass")
    classpath = sourceSets.main.runtimeClasspath
    systemProperties System.getProperties()
    args System.getProperty("exec.args", "").split()
}
```

## Running the CdapServiceNowToTxt pipeline example

Gradle 'executeCdapServiceNow' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapServiceNow -DmainClass=org.apache.beam.examples.complete.cdap.servicenow.CdapServiceNowToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

To execute this pipeline, specify the parameters in the following format:

```bash
 --clientId=your-client-id \
 --clientSecret=your-client-secret \
 --user=your-user \
 --password=your-password \
 --restApiEndpoint=your-endpoint \
 --queryMode=Table \
 --tableName=your-table \
 --valueType=Actual \
 --referenceName=your-reference-name \
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix
```

Please see CDAP [ServiceNow Batch Source](https://github.com/data-integrations/servicenow-plugins/blob/develop/docs/ServiceNow-batchsource.md) for more information.
