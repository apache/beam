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

# How to run a python nexmark benchmark

## Batch Mode

For batch mode, a file needs to be generated first by running java suite and writing events to a file.

```shell script
./gradlew :sdks:java:testing:nexmark:run \
    -Pnexmark.runner=":runners:direct-java"
    -Pnexmark.args=" --query=1 --runner=DirectRunner --streaming=false --suite=SMOKE --numEvents=100000  --manageResources=false --monitorJobs=true --enforceEncodability=true --enforceImmutability=true --generateEventFilePathPrefix=YOUR_FILE_PREFIX"
```

### Direct Runner

```shell script
python nexmark_launcher.py --query 5 --num_events 10000 --runner DirectRunner --input PATH_TO_INPUT_FILE
```

### Dataflow Runner

```shell script
python nexmark_launcher.py --query 5 --num_events 100000 --runner DataflowRunner --project PROJECT_NAME --region YOUR_REGION --temp_location TEMP_LOCATION --staging_location STAGING_LOCATION --sdk_location SDK_LOCATION --input PATH_TO_INPUT_FILE_ON_GS
```

## Streaming mode

First generate and publish events to pubsub using java nexmark suite, exmaple:
```shell script
./gradlew :sdks:java:testing:nexmark:run \
    -Pnexmark.runner=":runners:google-cloud-dataflow-java"
    -Pnexmark.args=" --runner=DataflowRunner --suite=SMOKE --streamTimeout=60 --query=0 --streaming=true --project=apache-beam-testing --region=YOUR_REGION --workerMachineType=n1-highmem-8 --gcpTempLocation=YOUR_TEMP_LOCATION --stagingLocation=YOUR_STAGING_LOCATION --sourceType=PUBSUB --pubSubMode=PUBLISH_ONLY --pubsubTopic=YOUR_TOPIC_NAME --resourceNameMode=VERBATIM --manageResources=false --monitorJobs=false --numEventGenerators=64 --numWorkers=16 --maxNumWorkers=16 --firstEventRate=50000 --nextEventRate=50000 --isRateLimited=true --avgPersonByteSize=500 --avgAuctionByteSize=500 --avgBidByteSize=500 --probDelayedEvent=0.000001 --occasionalDelaySec=60 --numEvents=3000000 --experiments=enable_custom_pubsub_sink --pubsubMessageSerializationMethod=TO_STRING"
```

### Direct Runner

```shell script
python nexmark_launcher.py --query 5 --num_events 3000000 --streaming --runner DirectRunner --topic_name YOUR_TOPIC_NAME --subscription_name YOUR_SUB_NAME --project YOUR_PROJECT_NAME --region YOUR_REGION
```

### Dataflow Runner

```shell script
python nexmark_launcher.py --query 5 --num_events 3000000 --streaming --runner DataflowRunner --num_workers 16 --machine_type n1-highmem-8 --topic_name YOUR_TOPIC_NAME --subscription_name YOUR_SUB_NAME --project YOUR_PROJECT_NAME --region YOUR_REGION --temp_location YOUR_TEMP_LOCATION --staging_location YOUR_STAGING_LOCATION --sdk_location YOUR_SDK_LOCATION
```
