---
title:  "Build a scalable, self-managed streaming infrastructure with Beam and Flink"
date:   2023-11-03 09:00:00 -0400
categories:
  - blog
authors:
  - talat
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

In this blog series, [Talat Uyarer (Senior Principal Engineer)](https://www.linkedin.com/in/talatuyarer/), [Rishabh Kedia (Principal Engineer)](https://www.linkedin.com/in/rishabhkedia/), and [David He (Engineering Director)](https://www.linkedin.com/in/davidqhe/) describe how we built a self-managed streaming platform by using Apache Beam and Flink. In this part of the series, we describe why and how we built a large-scale, self-managed streaming infrastructure and services based on Flink by migrating from a cloud managed streaming service. We also outline the learnings for operational scalability and observability, performance, and cost effectiveness. We summarize techniques that we found useful in our journey.

<!--more-->

# How to Build a Scalable Self Managed Streaming Infrastructure with Flink - Part 1

# Introduction

Palo Alto Networks (PANW) is a leader in cybersecurity, providing products, services and solutions to our customers. Data is the center of our products and services. We stream and store exabytes of data in our data lake, with near real-time ingestion, data transformation, data insertion to data store, and forwarding data to our internal ML-based systems and external SIEM’s. We support multi-tenancy in each component so that we can isolate tenants and provide optimal performance and SLA. Streaming processing plays a critical role in the pipelines.

In this blog series, [Talat Uyarer (Senior Principal Engineer)](https://www.linkedin.com/in/talatuyarer/), [Rishabh Kedia (Principal Engineer)](https://www.linkedin.com/in/rishabhkedia/), and [David He (Engineering Director)](https://www.linkedin.com/in/davidqhe/) will describe how we built a self managed streaming platform and our learnings. In part I, we describe why and how we built a large-scale self managed streaming infrastructure and services based on Flink, by migrating from a cloud managed streaming service, and the learnings for operational scalability and observability, performance, and cost effectiveness. We summarize useful techniques, and experience in our journey.

In Part 2, we will give a deeper description of our core building blocks of streaming infrastructure, such as autoscaler. We will also give more details on our heavy customization, so that we can build a high performance and large scale streaming system, along with the learnings of solving challenging problems.


# Why Self Managed Streaming Infrastructure Matters

In the past several years, we have built a large scale data platform on GCP. We used GCP Dataflow as a managed streaming service including the streaming engine running our application Apache Beam codes, and observability and tools such as Cloud Logging and Cloud Monitoring, more details can be referenced to [1]. The system can handle 15 millions of events per second, 1 trillion events daily, at 4 petabytes of data volume daily. We run ~30,000 Dataflow jobs, each job can have 1 or hundreds of workers depending on customer’s event throughputs. We support various applications using different endpoints: BigQuery data store, HTTPs based external SIEMs or internal endpoints, Syslog based SIEMs, GCS endpoints. Our customers and products rely on this data platform to handle cybersecurity postures and reactions. Our streaming infrastructure is highly flexible to add/update/delete a use case through a streaming job subscription. As an example of the use cases, a customer wants to ingest log events from a Firewall device into the data lake buffered in Kafka topics, and a streaming job is subscribed to extract/filter, transform data format, and do streaming insert to our BigQuery data warehouse endpoint in real-time, so that the customer through our visualization/dashboard products can view traffics or threads captured by this Firewall. This diagram illustrates the event producer and use case subscription workflow, and key components of the streaming platform:



<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/image1.png"
alt="Streaming service design">


This managed Dataflow based streaming infrastructure runs fine, but with some caveats:



1. Cost is high as it is a managed service. For the same resources used in a Dataflow application, such as vCPU and memory, the cost is much more expensive than using an open source streaming engine such as Flink running the same Beam application code.
2. Not easy to extend features, such as autoscaling based on different applications, endpoints, or different parameters within one same application, so that we can achieve our goals of ladency of SLA.
3. Runs only on GCP.

Another important factor we want to use a self managed service is the uniqueness of PANW’s streaming use cases. We support multi-tenancy. A tenant (a customer) can ingest data at a very high rate (>100k requests per seconds), or very low rate (&lt; 100 requests per second). A Dataflow job runs on VM’s instead of Kubernate, requiring a minimal one vCPU core. With a small tenant, this is just wasting resources. Our streaming infrastructure supports thousands of jobs, the CPU utilization will be more efficient if we do not have to use one core for a job.  It is natural for us to use a streaming engine running on Kubernetes, so that we can allocate minimal resources for a small tenant, for example, using a GKE POD with ½ or less vCPU core.


# Choosing Apache Flink and Kubernetes

In an effort to handle the above problems, we evaluated various streaming frameworks, including Apache Samza, Apache Flink, and Apache Spark, against GCP Dataflow to find the most efficient solution.

**Performance:**



* One standout factor was Apache Flink’s native Kubernetes support. Unlike Samza, which lacked native Kubernetes support and required Apache Zookeeper for coordination, Flink seamlessly integrated with Kubernetes. This eliminated unnecessary complexities. Performance-wise, both Samza and Flink were close competitors.
* Apache Spark, while popular, proved to be significantly slower in our tests. A presentation at the Beam Summit revealed that Apache Beam’s Spark Runner was approximately 10 times slower than Native Apache Spark [3]. We could not afford such a drastic performance hit and rewriting our entire Beam codebase with native Spark was not a viable option, especially given the extensive codebase we had built over the past four years with Apache Beam.

**Community:**



* The robustness of community support played a pivotal role in our decision-making. GCP Dataflow had provided excellent support, but we needed assurance in our choice of an open-source framework. Apache Flink’s vibrant community and active contributions from multiple companies offered a level of confidence that was unmatched. This collaborative environment meant that bug identification and fixes were ongoing processes. In fact, in our journey, we have patched our system using many Flink fixes from the community such as fixing the gcs file reading exceptions by merging Flink 1.15 open source fix [FLINK-26063](https://issues.apache.org/jira/browse/FLINK-26063?page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel&focusedCommentId=17504555#comment-17504555) (we are using 1.13), and worker restarting issue for stateful jobs from [FLINK-31963](https://issues.apache.org/jira/browse/FLINK-31963). We also found and fixed some bugs on the open source and contributed back to the community during our journey, such as [FLINK-32700](https://issues.apache.org/jira/browse/FLINK-32700) for Flink Kubernetes Operator. We created a new GKE Auth support for Kubernetes client and merged to github at [4].

**Integration:**



* Additionally, the seamless integration of Apache Flink with Kubernetes provided us with a flexible and scalable platform for orchestration. The synergy between Apache Flink and Kubernetes not only optimized our data processing workflows but also future-proofed our system.


# Architecture and Deployment Workflow

In the realm of real-time data processing and analytics, Apache Flink stands tall as a powerful and versatile framework. When combined with Kubernetes, the industry-standard container orchestration system, Flink applications can scale horizontally and enjoy robust management capabilities. We explore a cutting-edge design where Apache Flink and Kubernetes synergize seamlessly, thanks to the Apache Flink Kubernetes Operator.

At its core, the Flink Kubernetes Operator serves as a control plane, mirroring the knowledge and actions of a human operator managing Flink deployments. Unlike traditional methods, the Operator automates critical activities, from starting and stopping applications to handling upgrades and errors. Its versatile feature set includes fully-automated job lifecycle management, support for different Flink versions, and multiple deployment modes like application clusters and session jobs. Moreover, the Operator's operational prowess extends to metrics, logging, and even dynamic scaling via the Job Autoscaler.


## Building a Seamless Deployment Workflow

Imagine a robust system where Flink jobs are deployed effortlessly, monitored diligently, and managed proactively. This is precisely what our team achieved by integrating Apache Flink, Apache Flink Kubernetes Operator, and Kubernetes. Central to this setup is our custom-built Apache Flink Kubernetes Operator Client Library. This library acts as a bridge, enabling atomic operations such as starting, stopping, updating, and canceling Flink jobs.



<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/stream-service-changes.png"
alt="Streaming service changes">



## The Deployment Process

In our code, the client provides Apache Beam Pipeline Options, which include essential information such as the Kubernetes cluster's API endpoint, authentication details, GCP/S3 temporary location for uploading the JAR file, and worker type specifications. The Kubernetes Operator Library utilizes this information to orchestrate a seamless deployment process, We broke down steps below, note most of the core steps are automated in our code base:

**Step1: **



* Client wants to start a job for a customer and a specific application.

**Step 2:**



* **Generating a Unique Job ID**: The library generates a unique job ID, which is set as a Kubernetes label. This identifier helps in tracking and managing the deployed Flink job.
* **Configuration and Code Upload**: The library takes care of uploading all necessary configurations and user code to a designated location on Google Cloud Storage (GCS) or Amazon S3. This ensures that the Flink application's resources are readily available for deployment.
* **YAML Payload Generation**: Once the upload process is complete, the library constructs a YAML payload. This payload contains crucial deployment information, including resource settings determined based on the specified worker type.

In terms of worker VM instance type naming convention I would like to mention a little bit. We borrow a similar naming convention that Google Cloud has today. The naming convention n1-standard-1 refers to a specific predefined VM machine type. Let’s break down what each component of the name means:



* **n1**: This indicates cpu type of the instance. In this case, it refers to the intel based on instances in the N1 series. Google Cloud Platform has multiple generations of instances with varying hardware and performance characteristics.
* **standard**: This signifies the machine type family. "Standard" machine types offer a balanced ratio of 1 virtual CPU and 4 GB of memory for Task Manager and 0.5 vCPU and 2 GB memory for Job Manager
* **1**: This represents the number of virtual CPUs (vCPUs) available in the instance. In the case of n1-standard-1, it means the instance has 1 virtual CPU.

**Step 3:**



* **Calling Kubernetes API with Fabric8**: To initiate the deployment, the library interacts with the Kubernetes API using Fabric8. It's worth noting that Fabric8 initially lacked support for authentication in Google Kubernetes Engine (GKE) or Amazon Elastic Kubernetes Service (EKS). To address this limitation, our team implemented the necessary authentication support, which can be found in our Merge Request at GitHub PR [4].

**Step 4:**



* **Flink Operator Deployment**: Upon receiving the YAML payload, the Flink Operator takes charge of deploying the various components of the Flink job. This includes provisioning resources and managing the deployment of the Flink Job Manager, Task Manager, and Job Service.

**Step 5:**



* **Job Submission and Execution**: Once the Flink Job Manager is up and running, it fetches the JAR file and configurations from the designated GCS or S3 location. With all necessary resources in place, it submits the Flink job to the Standalone Flink Cluster for execution.

**Step 6**



* **Continuous Monitoring**: Post-deployment, our operator continuously monitors the status of the running Flink job. This real-time feedback loop enables us to promptly address any issues that may arise, ensuring the overall health and optimal performance of our Flink applications.

In summary, our deployment process leverages Apache Beam Pipeline Options, integrates seamlessly with Kubernetes and the Flink Operator, and employs custom logic to handle configuration uploads and authentication. This end-to-end workflow ensures a reliable and efficient deployment of Flink applications in Kubernetes clusters while maintaining vigilant monitoring for smooth operation. We created a sequence diagram to follow the steps below.

<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/job-start-activity-diagram.png"
alt="Job Start Activity Diagram">



# Developing an Autoscaler

Autoscaler is the key of the self-managed streaming service. There are not enough resources available on the internet for us to learn to build our own autoscaler which makes this problem even more tricky.

Autoscaler needs to scale up the number of task managers to drain the lag and keep up with the throughput. It will also scale down the minimum number of resources required to process the incoming traffic to save cost. We need to do this frequently while keeping the processing disruption to minimum.

We have tuned the autoscaler intensively, so that we can meet the SLA for latency, and make trade off for cost. We also make the autoscaler application specific so that we can meet specific needs for certain applications. Every decision has a hidden cost.  We dive into the great details of autoscaler in Part 2 of this blog.


# Creating a Client Library for Steaming Job Development

To deploy the job via Flink Kubernetes Operator, one has to be knowledgeable enough in the workings of kubernetes. The steps to create a single Flink job is:



1. Define a yaml file with proper specifications. This is an example:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: basic-reactive-example
spec:
  image: flink:1.13
  flinkVersion: v1_13
  flinkConfiguration:
    scheduler-mode: REACTIVE
    taskmanager.numberOfTaskSlots: "2"
    state.savepoints.dir: file:///flink-data/savepoints
    state.checkpoints.dir: file:///flink-data/checkpoints
    high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
    high-availability.storageDir: file:///flink-data/ha
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  podTemplate:
    spec:
      containers:
        - name: flink-main-container
          volumeMounts:
          - mountPath: /flink-data
            name: flink-volume
      volumes:
      - name: flink-volume
        hostPath:
          # directory location on host
          path: /tmp/flink
          # this field is optional
          type: Directory
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: savepoint
    state: running
    savepointTriggerNonce: 0
  mode: standalone
```

2. SSH inside your Flink cluster and run the command:

```
kubectl create -f job1.yaml
```


3. Check the status of the job:

```
kubectl get flinkdeployment job1
```



This process impacts our scalability. We frequently update our jobs so It is not feasible to do this manually for thousands of running jobs, as it's highly error prone and slow. One wrong spacing in the yaml can fail the deployment. This approach also acts as a barrier to innovation as one must know kubernetes to interact with Flink jobs.

To solve these, we decided to build a library to provide an interface for any teams and applications wanting to start, delete, update or get status of their jobs.

<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/fko-library.png"
alt="Flink Kubernetes Operator Library">


This library extends Fabric8 client and FlinkDeployment CRD. FlinkDeployment CRD is exposed by Flink Kubernetes Operator. CRD lets you store and retrieve structured data. By extending the CRD, we get access to POJO, making it easier to manipulate the yaml file.

It will:



1. Authenticate - ensure you are allowed to perform actions on the Flink cluster
2. Validate (Fetches template from AWS/GCS for validation) - will take user variable input and validate it against the policy and rules and yaml format.
3. Execute the action - convert the java call to invoke kubernetes operation

Lessons learned:



1. App specific operator service - at our large scale, the operator was not able to handle such a large amount of jobs. Kubernetes calls started to time out and failed. To solve this, we created multiple operators (~4) in high traffic regions to handle each application.
2. Kube call caching - Cache results of kubernetes calls for 30-60s to prevent overloading
3. Label support - providing label support to search jobs via client specific variables reduced the load on Kube along with 5x faster search of the jobs

Some of our biggest wins by exposing the library are:



1. Standardized Job Management - Users can easily start, delete, and get status updates of their Flink jobs in a Kubernetes environment using a single library.
2. Abstracted Kubernetes Complexity - Teams no longer need to worry about the inner workings of Kubernetes or formatting job deployment YAML files, as the library handles these details internally.
3. Easy Upgrades - With the underlying Kubernetes infrastructure, the library brings robustness and fault tolerance to Flink job management, ensuring minimal downtime and efficient recovery.


# Observability and Alerting

Observability is important to run the production system at a large scale. We have ~30k streaming jobs in PANW, each one serves a customer for a specific application. Each job reads data from multiple topics in kafka, performs transformation and then writes the data to various sinks/endpoints.

The constraint can be anywhere in the pipeline or its endpoints (Customer api, BigQuery, etc). We want to make sure the latency of streaming meets the SLA. Therefore, understanding if a job is healthy, meeting SLA or not, alerting/intervening if needed is very challenging.

We build a sophisticated observability and alerting capability to achieve our operational goals. We provide 3 kinds of observability and debugging tools, described below.


## Flink job list and job insight from Prometheus and Grafana

Each Flink job sends various metrics to our Prometheus with great cardinality details, such as application name, customer id, regions, so that we can slice and dice to look at each job. Critical metrics include input traffic rate, output throughput, backlogs in Kafka, timestamp based latency, task CPU usage, task numbers, OOM counts, etc. Following charts are just a few examples: the charts give us details of ingestion traffic rate to Kafka for a specific customer, streaming job’s overall throughput and each vCPU’s throughput, backlogs in Kafka, worker autoscaling based on the observed backlog.

<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/job-metrics.png"
alt="Flink Job Metrics">

<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/autoscaling-metrics.png"
alt="Flink Job Autoscaling Metrics">


Streaming latency based on timestamp watermark: not only the numbers of events in Kafka as backlogs, it is also important to know the time latency for streaming end-to-end so that we can define and monitor SLA. The latency is defined as the time taken for the streaming processing, starting from ingestion timestamp, to the timestamp sending to streaming endpoint.  A watermark is the last processed event’s time. With the watermark, we are tracking P100 latency. We track each event’s stream latency, so that we can understand each Kafka topic/partition or Flink job pipeline issue. This is an example of each event stream and its latency:

<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/watermark-metrics.png"
alt="Apache Beam Watermark Metrics">


## Flink Open Source UI

We use and extend Apache Flink Dashboard UI so that we can closely monitor jobs and tasks, such as the  checkpoint duration, size, and failure. One important extension we did is a job history page so that we can look back at a job's start/update timeline and details so that we can debug issues.

<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/flink-checkpoint-ui.png"
alt="Flink Checkpoint UI">



## Dashboards and Alerting for Backlog and Latency

We have ~30k jobs, and we want to closely monitor and alert those jobs which run into abnormal states, so that we can quickly intervene. We have created dashboards for each application so that we can show the top latency list, and alert them based on thresholds. This is one example of the timestamp based latency dashboard for one application. We can set the alerting if the latency is larger than a threshold, such as 10 minutes, for a certain time continuously:


<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/latency-graph.png"
alt="Latency Graph">


Another example of backlogs based dashboards:


<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/backlog-graph.png"
alt="Backlog Graph">


Alerting is threshold based with frequent metrics checking. If a threshold is met, and continues for a certain amount of times, we will alert our internal Slack channels, or PagerDuty for immediate attention. We tune the alerting so that the accuracy is high.


# Cost Optimization: Strategies and Tuning

A major goal for us to move to a self managed streaming service is cost efficiency. We have achieved the goal by saving more than half of the cost by several minor tunings. We can see more room for improvements further.

These are just a few tips in our journey so far:



1. Using GCS as Checkpointing storage.
2. Reducing the write frequency to GCS
3. Freedom to use different machine types. For example, in GCP N2D machines are 15% cheaper than N2 machines.
4. Autoscaling tasks to use optimal resources while maintaining latency SLA (we will cover the details in Part 2)

We describe more details for #1 and #2 related to checkpointing optimization as follows.

**Google Cloud Storage and Checkpointing**

GCS serves as our checkpoint store due to its cost-effectiveness, scalability, and durability. When working with GCS, several design considerations and best practices can optimize scaling and performance:



* Data partitioning methods like range partitioning, which divides data based on specific attributes, and hash partitioning, distributing data evenly using hash functions, are crucial.
* Avoiding sequential key names, especially timestamps, helps prevent hotspots and uneven data distribution; instead, introduce random prefixes for object distribution.
* Employing a hierarchical folder structure enhances data management and reduces the number of objects in a single directory.
* For small files, combining them into larger ones improves read throughput, while minimizing small files reduces inefficient storage use and metadata operations.

**Tune the frequency writing the GCS**

One of our primary challenges lay in scaling jobs efficiently. Stateless jobs, which are relatively simpler, still presented hurdles, especially in scenarios where Flink needed to process an overwhelming number of workers. To overcome this challenge, We increased state.storage.fs.memory-threshold settings to 1 MB from 20KB (??). This configuration allowed us to combine small checkpoints files into larger ones at Job Manager level and reduce metadata calls.

Another challenge we tackled was optimizing the performance of GCS operations. While GCS is excellent for streaming large amounts of data, it does have limitations when it comes to handling high-frequency I/O requests. To mitigate this, we employed strategies like introducing random prefixes in key names, avoiding sequential key names, and optimizing our GCS sharding techniques. These methods significantly enhanced our GCS performance, enabling smooth operation of our stateless jobs.

This is the chart showing the GCS writes reduction after changing the memory-threshold:



<img class="center-block"
src="/images/blog/apache-beam-flink-and-kubernetes/gcs-write-graph.png"
alt="GCS write Graph">



# Conclusion

Palo Alto Networks® Cortex Data Lake is fully migrated from Dataflow streaming engine to Flink self managed streaming engine infrastructure. We have achieved our goals to run the system more cost efficiently (more than half cost cut), and run the infrastructure on multiple clouds such as GCP and AWS. We have learned how to build a large scale reliable production system based on open sources. We see large potentials to customize the system based on our specific needs as we have a lot of freedom to customize the open source code and configuration. In the next Part 2 post we will give more details on autoscaling and performance tuning parts. We hope our experience will be helpful for readers who will explore similar solutions for their own organizations.


# Additional Resources

We provide links here for related presentations as further reading for readers interested in implementing similar solutions. By adding this section, we hope you can find more details to build a fully managed streaming infrastructure, making it easier for readers to follow our stories and learnings.

[1] Streaming framework at PANW published at Apache Beam: [https://beam.apache.org/case-studies/paloalto/](https://beam.apache.org/case-studies/paloalto/)

[2] PANW presentation at Beam Summit 2023: [https://youtu.be/IsGW8IU3NfA?feature=shared](https://youtu.be/IsGW8IU3NfA?feature=shared)

[3] Benchmark presented at Beam Summit 2021: [https://2021.beamsummit.org/sessions/tpc-ds-and-apache-beam/](https://2021.beamsummit.org/sessions/tpc-ds-and-apache-beam/)

[4] PANW open source contribution to Flink for GKE Auth support: [https://github.com/fabric8io/kubernetes-client/pull/4185](https://github.com/fabric8io/kubernetes-client/pull/4185)


# Acknowledgements

This is a large effort to build the new infrastructure and to migrate the large customer based applications from cloud provider managed streaming infrastructure to self-managed Flink based infrastructure at scale. Thanks the Palo Alto Networks CDL streaming team who helped to make this happen: Kishore Pola, Andrew Park, Hemant Kumar, Manan Mangal, Helen Jiang, Mandy Wang, Praveen Kumar Pasupuleti, JM Teo, Rishabh Kedia, Talat Uyarer, Naitk Dani, and David He.
