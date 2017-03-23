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

# Running NexMark on Beam on Flink on Google Compute Platform

Here's how to create a cluster of VMs on Google Compute Platform, deploy
Flink to them, and invoke a NexMark pipeline using the Beam-on-Flink
runner.

These instructions are somewhat baroque and I hope they can be
simplified over time.

## Prerequisites

You'll need:

* the Google Cloud SDK
* a clone of the Beam repository
* a Flink binary distribution
* a project on Google Compute Platform.

## Establish the shell environment

```
# Beam root
BEAM=<path to Beam source directory>
# Flink root
FLINK_VER=flink-1.0.3
FLINK=<path to Flink distribution directory>
# Google Cloud project
PROJECT=<your project id>
# Google Cloud zone
ZONE=<your project zone>
# Cloud commands
GCLOUD=<path to gcloud command>
GSUTIL=<path to gsutil command>
```

## Establish VM names for Flink master and workers

```
MASTER=flink-m
NUM_WORKERS=5
WORKERS=""
for (( i = 0; i < $NUM_WORKERS; i++ )); do
  WORKERS="$WORKERS flink-w-$i"
done
ALL="$MASTER $WORKERS"
```

## Build Beam

```
( cd $BEAM && mvn clean install )
```

## Bring up the cluster

Establish project defaults and authenticate:
```
$GCLOUD init
$GCLOUD auth login
```

Build Google Cloud Dataproc cluster:
```
$GCLOUD beta dataproc clusters create \
  --project=$PROJECT \
  --zone=$ZONE \
  --bucket=nexmark \
  --scopes=cloud-platform \
  --num-workers=$NUM_WORKERS \
  --image-version=preview \
  flink
```

Force google_compute_engine ssh keys to be generated locally:
```
$GCLOUD compute ssh \
  --project=$PROJECT \
  --zone=$ZONE \
  $MASTER \
  --command 'exit'
```

Open ports on the VMs:
```
$GCLOUD compute firewall-rules create allow-monitoring --allow tcp:8080-8081
$GCLOUD compute firewall-rules create allow-debug --allow tcp:5555
```

Establish keys on master and workers
**CAUTION:** This will leave your private key on your master VM.
Better would be to create a key just for inter-worker ssh.
```
for m in $ALL; do
  echo "*** $m ***"
  $GCLOUD beta compute scp \
    --project=$PROJECT \
    --zone=$ZONE \
    ~/.ssh/google_compute_engine.pub $m:~/.ssh/
done
$GCLOUD beta compute scp \
  --project=$PROJECT \
  --zone=$ZONE \
  ~/.ssh/google_compute_engine $MASTER:~/.ssh/
```

Collect IP addresses for workers:
```
MASTER_EXT_IP=$($GCLOUD compute instances describe \
 --project=$PROJECT \
  --zone=$ZONE \
  $MASTER | grep natIP: | sed 's/[ ]*natIP:[ ]*//')
MASTER_IP=$($GCLOUD compute instances describe \
 --project=$PROJECT \
  --zone=$ZONE \
  $MASTER | grep networkIP: | sed 's/[ ]*networkIP:[ ]*//')
WORKER_IPS=""
for m in $WORKERS; do
  echo "*** $m ***"
  WORKER_IP=$($GCLOUD compute instances describe \
    --project=$PROJECT \
    --zone=$ZONE \
    $m | grep networkIP: | sed 's/[ ]*networkIP:[ ]*//')
  WORKER_IPS="$WORKER_IPS $WORKER_IP"
done
```

Configure Flink:
```
cat $FLINK/conf/flink-conf.yaml \
  | sed "s|.*\(jobmanager.rpc.address\):.*|\1: $MASTER_IP|g" \
  | sed "s|.*\(jobmanager.heap.mb\):.*|\1: 4096|g" \
  | sed "s|.*\(taskmanager.heap.mb\):.*|\1: 8192|g" \
  | sed "s|.*\(parallelism.default\):.*|\1: $(($NUM_WORKERS * 4))|g" \
  | sed "s|.*\(fs.hdfs.hadoopconf\):.*|\1: /etc/hadoop/conf|g" \
  | sed "s|.*\(taskmanager.numberOfTaskSlots\):.*|\1: 4|g" \
  | sed "s|.*\(jobmanager.web.submit.enable\):.*|\1: false|g" \
  | sed "s|.*\(env.ssh.opts\):.*||g" \
  > ~/flink-conf.yaml
cat $FLINK/conf/log4j.properties \
  | sed "s|.*\(log4j.rootLogger\)=.*|\1=ERROR, file|g" \
  > ~/log4j.properties
echo "env.ssh.opts: -i /home/$USER/.ssh/google_compute_engine -o StrictHostKeyChecking=no" >> ~/flink-conf.yaml
echo "$MASTER_IP:8081" > ~/masters
echo -n > ~/slaves
for ip in $WORKER_IPS; do
  echo $ip >> ~/slaves
done
cp -f \
  ~/flink-conf.yaml \
  ~/masters ~/slaves \
  ~/log4j.properties \
  $FLINK/conf/
```

Package configured Flink for distribution to workers:
```
( cd ~/ && tar -cvzf ~/flink.tgz $FLINK/* )
```

Distribute:
```
$GSUTIL cp ~/flink.tgz gs://nexmark
for m in $ALL; do
  echo "*** $m ***"
  $GCLOUD compute ssh \
    --project=$PROJECT \
    --zone=$ZONE \
    $m \
    --command 'gsutil cp gs://nexmark/flink.tgz ~/ && tar -xvzf ~/flink.tgz'
done
```

Start the Flink cluster:
```
$GCLOUD compute ssh \
  --project=$PROJECT \
  --zone=$ZONE \
  $MASTER \
  --command "~/$FLINK_VER/bin/start-cluster.sh"
```

Bring up the Flink monitoring UI:
```
/usr/bin/google-chrome $MASTER_EXT_IP:8081 &
```

## Run NexMark

Distribute the Beam + NexMark jar to all workers:
```
$GSUTIL cp $BEAM/integration/java/target/java-integration-all-bundled-0.2.0-incubating-SNAPSHOT.jar gs://nexmark
for m in $ALL; do
  echo "*** $m ***"
  $GCLOUD compute ssh \
    --project=$PROJECT \
    --zone=$ZONE \
    $m \
    --command "gsutil cp gs://nexmark/java-integration-all-bundled-0.2.0-incubating-SNAPSHOT.jar ~/$FLINK_VER/lib/"
done
```

Create a Pubsub topic and subscription for testing:
```
$GCLOUD alpha pubsub \
  --project=$PROJECT \
  topics create flink_test

$GCLOUD alpha pubsub \
  --project=$PROJECT \
  subscriptions create flink_test \
  --topic flink_test \
  --ack-deadline=60 \
  --topic-project=$PROJECT
```

Launch!
**NOTE:** As of flink-1.0.3 this will throw a `NullPointerException`
in `org.apache.beam.sdk.io.PubsubUnboundedSink$WriterFn.startBundle`.
See Jira issue [BEAM-196](https://issues.apache.org/jira/browse/BEAM-196).

```
$GCLOUD compute ssh \
  --project=$PROJECT \
  --zone=$ZONE \
  $MASTER \
  --command "~/$FLINK_VER/bin/flink run \
  -c org.apache.beam.integration.nexmark.drivers.NexmarkFlinkDriver \
  ~/$FLINK_VER/lib/java-integration-all-bundled-0.2.0-incubating-SNAPSHOT.jar \
  --project=$PROJECT \
  --streaming=true \
  --query=0 \
  --sourceType=PUBSUB \
  --pubSubMode=COMBINED \
  --pubsubTopic=flink_test \
  --resourceNameMode=VERBATIM \
  --manageResources=false \
  --monitorJobs=false \
  --numEventGenerators=5 \
  --firstEventRate=1000 \
  --nextEventRate=1000 \
  --isRateLimited=true \
  --numEvents=0 \
  --useWallclockEventTime=true \
  --usePubsubPublishTime=true"
```

## Teardown the cluster

Stop the Flink cluster:
```
$GCLOUD compute ssh \
  --project=$PROJECT \
  --zone=$ZONE \
  $MASTER \
  --command "~/$FLINK_VER/bin/stop-cluster.sh"
```

Teardown the Dataproc cluster:
```
$GCLOUD beta dataproc clusters delete \
  --project=$PROJECT \
  flink
```
