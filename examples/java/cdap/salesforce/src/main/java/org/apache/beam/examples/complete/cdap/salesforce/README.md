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
task executeCdapSalesforce (type:JavaExec) {
    mainClass = System.getProperty("mainClass")
    classpath = sourceSets.main.runtimeClasspath
    systemProperties System.getProperties()
    args System.getProperty("exec.args", "").split()
}
```

## Running the CdapSalesforceToTxt pipeline example

Gradle 'executeCdapSalesforce' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapSalesforce -DmainClass=org.apache.beam.examples.complete.cdap.salesforce.CdapSalesforceToTxt \
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

Gradle 'executeCdapSalesforce' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapSalesforce -DmainClass=org.apache.beam.examples.complete.cdap.salesforce.TxtToCdapSalesforce \
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

Gradle 'executeCdapSalesforce' task allows to run the pipeline via the following command:

```bash
gradle clean executeCdapSalesforce -DmainClass=org.apache.beam.examples.complete.cdap.salesforce.CdapSalesforceStreamingToTxt \
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
- `pushTopicName` - name of the push topic that was created from query for some sObject. This push topic should have enabled *pushTopicNotifyCreate* property.
  If push topic with such name doesn't exist, then new push topic for provided **'sObjectName'** will be created automatically.
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
