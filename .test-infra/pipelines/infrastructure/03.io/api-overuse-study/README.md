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

# Overview

This directory holds terraform code and Kubernetes manifests to provision
resources needed to support the
[API Overuse Study](https://docs.google.com/document/d/1VZ9YphDO7kewBSz5oMXVPHWaib3S03Z6aZ66BhciB3E/edit?usp=sharing&resourcekey=0-ItxMSG72EzfSwVedSz-Zeg)

# Usage and Requirements

See [.test-infra/pipelines/infrastructure](../..) for general usage and
requirements of terraform modules.

# Specific Requirements

## Ko

These modules depend on [ko.build](https://ko.build/) to build Go container
images quickly.

## Kubernetes

Modules in this directory assume you are connected to a running Kubernetes
cluster, either
[minikube](https://minikube.sigs.k8s.io/) for local development or
Google Kubernetes Engine (GKE) for certain ingress
configurations (See [04.ingress](04.ingress)).

If you need a GKE cluster, this repository contains terraform to provision one.
See [.test-infra/terraform/google-cloud-platform/google-kubernetes-engine](../../../../terraform/google-cloud-platform/google-kubernetes-engine)
for details.
