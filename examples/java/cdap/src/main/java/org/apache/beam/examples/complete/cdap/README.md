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

# Apache Beam pipeline examples to ingest data from CDAP plugin to TXT file

This directory contains set of [Apache Beam](https://beam.apache.org/) pipeline examples that create a pipeline to read data
from a [CDAP](https://cdap.atlassian.net/wiki/spaces/DOCS/overview?homepageId=379748484) plugin
and write data into .txt file (and vice versa).

Supported CDAP plugins:

- [Hubspot](https://github.com/data-integrations/hubspot)
- [Salesforce](https://github.com/data-integrations/salesforce)
- [ServiceNow](https://github.com/data-integrations/servicenow-plugins)
- [Zendesk](https://github.com/data-integrations/zendesk)

## Gradle preparation

To run this example your `build.gradle` file should contain the following task to execute the pipeline:

```
task executeCdap (type:JavaExec) {
    mainClass = System.getProperty("mainClass")
    classpath = sourceSets.main.runtimeClasspath
    systemProperties System.getProperties()
    args System.getProperty("exec.args", "").split()
}
```

## Running the CdapHubspotToTxt pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.CdapHubspotToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

To execute this pipeline, specify the parameters in the following format:

```bash
 --apikey=your-api-key \
 --referenceName=your-reference-name \
 --objectType=Contacts \
 --txtFilePath=your-path-to-output-file
```

## Running the CdapHubspotStreamingToTxt pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.CdapHubspotStreamingToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

To execute this pipeline, specify the parameters in the following format:

```bash
 --apikey=your-api-key \
 --referenceName=your-reference-name \
 --objectType=Contacts \
 --txtFilePath=your-path-to-output-file
```

## Running the TxtToCdapHubspot pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.TxtToCdapHubspot \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

To execute this pipeline, specify the parameters in the following format:

```bash
 --apikey=your-api-key \
 --referenceName=your-reference-name \
 --objectType=your-object-type \
 --txtFilePath=your-path-to-input-file \
 --locksDirPath=your-path-to-locks-dir
```

## Running the CdapServiceNowToTxt pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.CdapServiceNowToTxt \
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
 --outputTxtFilePath=your-path-to-file
```

## Running the CdapSalesforceToTxt pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.CdapSalesforceToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

To execute this pipeline, specify the parameters in the following format:

```bash
 --username=your-user-name\
 --password=your-password \
 --securityToken=your-token \
 --consumerKey=your-key \
 --consumerSecret=your-secret \
 --loginUrl=your-login-url \
 --sObjectName=object-name \
 --referenceName=your-reference-name \
 --txtFilePath=your-path-to-file
```

## Running the TxtToCdapSalesforce pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.TxtToCdapSalesforce \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

To execute this pipeline, specify the parameters in the following format:

```bash
 --username=your-user-name\
 --password=your-password \
 --securityToken=your-token \
 --consumerKey=your-key \
 --consumerSecret=your-secret \
 --loginUrl=your-login-url \
 --sObject=CustomObject__c \
 --referenceName=your-reference-name \
 --txtFilePath=your-path-to-file \
 --operation=Insert \
 --errorHandling=Stop on error \
 --maxRecordsPerBatch=10 \
 --maxBytesPerBatch=9999999 \
 --locksDirPath=your-path
```

## Running the CdapZendeskToTxt pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.CdapZendeskToTxt \
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
 --outputTxtFilePath=your-path-to-file
```
