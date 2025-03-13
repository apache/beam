<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
-->

# GitHub Actions - Self-hosted Windows Runners

This folder contains the required resources to deploy the GitHub Actions self-hosted runners for the workflows running in Windows OS.

#### How to build a new instance template for the instance group?

* Create a new VM Instance using Windows 2019 Datacenter OS with at least 70GB of disk, 2vCPUs and 8 GB of RAM.

* Install the following software:
  * VS Build Tools 2019
  * MS Build Tools
  * Desktop Development with C++
  * Testing tool Core Features
  * Windows 10 SDK
  * Git 2.34.1.windows.1
  * Git LFS
  * Git Bash

* Create an image disk from the VM instance.

* Now that you have the image disk you can create an instance template by using it and adding the startup and shutdown scripts through the respective metadata fields.

**Be sure that you are using a service account that has permissions to invoke cloud functions.**

Now that the instance template is ready you can use it to either run it independently for an individual runner or create an instance group for a set of runners.