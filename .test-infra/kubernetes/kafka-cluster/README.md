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
## Kafka test cluster

The kubernetes config files in this directory create a Kafka cluster
comprised of 3 Kafka replicas and 3 Zookeeper replicas. They
expose themselves using 3 LoadBalancer services. To deploy the cluster, simply run

    sh setup-cluster.sh

Before executing the script, ensure that your account has cluster-admin
privileges, as setting RBAC cluster roles requires that.

The scripts are based on [Yolean kubernetes-kafka](https://github.com/Yolean/kubernetes-kafka)
