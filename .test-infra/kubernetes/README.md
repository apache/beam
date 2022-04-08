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
## Kubernetes
This folder contains resources required for some tests to deploy Kubernetes objects.

Most of the files are deployed and used by automated Beam Performance Tests in Jenkins, which also takes responsability of removing those after the process is finished


### Clusters Specification

#### io-datastores
* GKE Version: 1.22.6-gke.300
* Node Pool Version: 1.22.6-gke.300
* Runtime: Container-Optimized OS with containerd (cos_containerd)
* Authentication method: OAuth Client Certificate.



### Note: Basic Auth is not supported on BEAM Clusters as of March 4th 2022

Prior to v1.19 GKE allowed to log into a cluster using basic authentication methods, which relies on a username and password, and now it is deprecated.

Currently, OAuth is the standard authentication method, previous usernames and passwords were removed so only the certificate remains as master auth.

Some Beam Performance tests need to be logged into the cluster so they can create its own resources, the correct method to do so is by setting up the right Kubeconfig inside the worker and execute get credentials within GCP.

In the future if you need to create an automatic process that need to have access to the cluster, use OAuth inside your script or job.

