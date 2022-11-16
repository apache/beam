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

# Apache Beam pipeline examples for CdapIO and CDAP plugins

This directory contains set of [Apache Beam](https://beam.apache.org/) pipeline examples to read data
from a [CDAP plugin](https://github.com/data-integrations) and write data into .txt file (and vice versa).

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

`CdapHubspotToTxt` pipeline parameters:
- `apikey` - Hubspot OAuth2 API Key
- `objectType` - Hubspot objects to pull supported by CDAP [Hubspot Batch Source](https://github.com/data-integrations/hubspot/blob/develop/docs/Hubspot-batchsource.md)
- `outputTxtFilePathPrefix` - path to output folder with filename prefix. It will write a set of .txt files with names like {prefix}-###.

Please see CDAP [Hubspot Batch Source](https://github.com/data-integrations/hubspot/blob/develop/docs/Hubspot-batchsource.md) for more information.

To execute this pipeline, specify the parameters in the following format:

```bash
 --apikey=your-api-key \
 --referenceName=your-reference-name \
 --objectType=Contacts \
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix
```

## Running the CdapHubspotStreamingToTxt pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.CdapHubspotStreamingToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

`CdapHubspotStreamingToTxt` pipeline parameters:
- `apikey` - Hubspot OAuth2 API Key
- `objectType` - Hubspot objects to pull supported by CDAP [Hubspot Streaming Source](https://github.com/data-integrations/hubspot/blob/develop/docs/Hubspot-streamingsource.md)
- `outputTxtFilePathPrefix` - path to output folder with filename prefix. It will write a set of .txt files with names like {prefix}-###.
- `pullFrequencySec` - delay in seconds between polling for new records updates. (Optional)
- `startOffset` - inclusive start offset from which the reading should be started. (Optional)

Please see CDAP [Hubspot Streaming Source](https://github.com/data-integrations/hubspot/blob/develop/docs/Hubspot-streamingsource.md) for more information.

To execute this pipeline, specify the parameters in the following format:

```bash
 --apikey=your-api-key \
 --referenceName=your-reference-name \
 --objectType=Contacts \
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix \
 --pullFrequencySec=100 \
 --startOffset=1000
```

## Running the TxtToCdapHubspot pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.TxtToCdapHubspot \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

`TxtToCdapHubspot` pipeline parameters:
- `apikey` - Hubspot OAuth2 API Key
- `objectType` - Hubspot objects to pull supported by [Hubspot Streaming Sink](https://github.com/data-integrations/hubspot/blob/develop/docs/Hubspot-batchsink.md)
- `inputTxtFilePath` - input .txt file path
- `locksDirPath` - locks directory path where locks will be stored. This parameter is needed for Hadoop External Synchronization (mechanism for acquiring locks related to the write job).

Please see CDAP [Hubspot Streaming Source](https://github.com/data-integrations/hubspot/blob/develop/docs/Hubspot-streamingsource.md) for more information.

To execute this pipeline, specify the parameters in the following format:

```bash
 --apikey=your-api-key \
 --referenceName=your-reference-name \
 --objectType=your-object-type \
 --inputTxtFilePath=your-path-to-input-txt-file \
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
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix
```

Please see CDAP [ServiceNow Batch Source](https://github.com/data-integrations/servicenow-plugins/blob/develop/docs/ServiceNow-batchsource.md) for more information.

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
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix
```

Please see CDAP [Salesforce Batch Source](https://github.com/data-integrations/servicenow-plugins/blob/develop/docs/ServiceNow-batchsource.md) for more information.

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
 --inputTxtFilePath=your-path-to-txt-file \
 --operation=Insert \
 --errorHandling=Stop on error \
 --maxRecordsPerBatch=10 \
 --maxBytesPerBatch=9999999 \
 --locksDirPath=your-path
```
Please see CDAP [Salesforce Batch Sink](https://github.com/data-integrations/salesforce/blob/develop/docs/Salesforce-batchsink.md) for more information.

## Running the CdapSalesforceStreamingToTxt pipeline example

Gradle 'executeCdap' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdap -DmainClass=org.apache.beam.examples.complete.cdap.CdapSalesforceStreamingToTxt \
     -Dexec.args="--<argument>=<value> --<argument>=<value>"
```

`CdapSalesforceStreamingToTxt` pipeline parameters:
- `username` - Salesforce username.
- `password` - Salesforce user password.
- `securityToken` - Salesforce security token.
- `consumerKey` - Salesforce connected app's consumer key.
- `consumerSecret` - Salesforce connected app's consumer secret.
- `loginUrl` - Salesforce endpoint to authenticate to. Example: *'https://MyDomainName.my.salesforce.com/services/oauth2/token'*.
- `sObjectName` - Salesforce object to pull supported by CDAP Salesforce Streaming Source.
- `pushTopicName` - name of the push topic that was created from query for some sObject. 
If push topic with such name doesn't exist, then new push topic for provided **'sObjectName'** will be created.
- `pullFrequencySec` - delay in seconds between polling for new records updates. (Optional)
- `startOffset` - inclusive start offset from which the reading should be started. (Optional)

Please see [CDAP Salesforce](https://github.com/data-integrations/salesforce) for more information.
Also, please see documentation regarding Salesforce streaming API authorization [here](https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/code_sample_auth_oauth.htm).

To execute this pipeline, specify the parameters in the following format:

```bash
 --username=your-user-name\
 --password=your-password \
 --securityToken=your-token \
 --consumerKey=your-key \
 --consumerSecret=your-secret \
 --loginUrl=your-login-url \
 --sObjectName=object-name \
 --pushTopicName=your-topic-name \
 --pullFrequencySec=100 \
 --startOffset=1000
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
 --outputTxtFilePathPrefix=your-path-to-output-folder-with-filename-prefix
```
Please see CDAP [Zendesk Batch Source](https://github.com/data-integrations/zendesk/blob/develop/docs/Zendesk-batchsource.md) for more information.
