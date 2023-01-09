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
task executeCdapZendesk (type:JavaExec) {
    mainClass = System.getProperty("mainClass")
    classpath = sourceSets.main.runtimeClasspath
    systemProperties System.getProperties()
    args System.getProperty("exec.args", "").split()
}
```

## Running the CdapZendeskToTxt pipeline example

Gradle 'executeCdapZendesk' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapZendesk -DmainClass=org.apache.beam.examples.complete.cdap.zendesk.CdapZendeskToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

To execute this pipeline, specify the parameters in the following format:

```bash
 --zendeskBaseUrl=zendesk-url-key-followed-by-/%s/%s (example: https://support.zendesk.com/%s/%s) \
 --adminEmail=your-admin-admin-email \
 --apiToken=your-api-token \
 --subdomains=your-subdomains (example: api/v2) \
 --maxRetryCount=your-max-retry-count \
 --maxRetryWait=your-max-retry-wait \
 --maxRetryJitterWait=your-max-retry-jitter-wait \
 --connectTimeout=your-connection-timeout \
 --readTimeout=your-read-timeout \
 --objectsToPull=your-objects-to-pull (example: Groups) \
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix
```
Please see CDAP [Zendesk Batch Source](https://github.com/data-integrations/zendesk/blob/develop/docs/Zendesk-batchsource.md) for more information.
